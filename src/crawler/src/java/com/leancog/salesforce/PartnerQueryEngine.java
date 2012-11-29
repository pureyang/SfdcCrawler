package com.leancog.salesforce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;

import javax.xml.bind.DatatypeConverter;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leancog.salesforce.metadata.MetadataQueryEngine;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.bind.XmlObject;

/**
 * PartnerQueryEngine allows query into Salesforce using Partner WSDL SOAP API
 * 
 * findUpdates - find updated objects and their values
 * findDeleteds - find objects that are out of date
 * @author leancog
 *
 */
public class PartnerQueryEngine {

	private static final Logger LOG = LoggerFactory.getLogger(PartnerQueryEngine.class);
	
	private PartnerConnection connection = null;
	private int FETCH_LIMIT;
	private Date LAST_CRAWL;
	
	// contains all sfdc Article Types along with their default and custom fields, populated using sfdc metadata API 
	private ArrayList<SCustomObject> articleTypes = new ArrayList<SCustomObject>();
	// default SObject subfields
	private ArrayList<String> dataCategorySelectionsFields = new ArrayList<String>();
	// SObject fields that should be translated into full username
	private ArrayList<String> userIdToNameFields = new ArrayList<String>();
	// cache of sfdc userid to username
  private HashMap<String, String> sfdcUserIdToUserFullname = null; 
	
	public PartnerQueryEngine() {
	}
	
	public void initConnection(String username, String passwd, int fetchLimit, Date lastCrawl) 
	  throws ConnectionException{

    ConnectorConfig config = new ConnectorConfig();
    config.setUsername(username);
    LOG.info("Salesforce Crawler:Username: "+config.getUsername());
    config.setPassword(passwd);
    connection = Connector.newConnection(config);
    LOG.info("Salesforce Crawler:Auth EndPoint: "+config.getAuthEndpoint());
    LOG.info("Salesforce Crawler:Service EndPoint: "+config.getServiceEndpoint());
    
    FETCH_LIMIT = fetchLimit;
    LAST_CRAWL = lastCrawl;
    // reset usermapping cache on every run
    sfdcUserIdToUserFullname = new HashMap<String, String>();
    
    initializeMetaDataFields(username, passwd, config.getAuthEndpoint());
	}
	 
	public void logout() throws ConnectionException {
	  connection.logout();
	}
	
	public UpdateQueryResult findUpdates() {
    if (LAST_CRAWL == null) {
      LOG.info("Salesforce Crawler:Indexing INITIAL LOAD");     
    } else {
      LOG.info("Salesforce Crawler:Indexing since last crawl="+LAST_CRAWL.toString());
    }
  	UpdateQueryResult result = new UpdateQueryResult();
  	Iterator<SCustomObject> i = articleTypes.iterator();
  	while (i.hasNext()) {
  	  SCustomObject customObject = i.next();
  	  result.addResults(updateIndex(FETCH_LIMIT, customObject, dataCategorySelectionsFields, LAST_CRAWL));  
  	}

  	return result;
	}
	
	public ArrayList<String> findDeleteds() {
	  ArrayList<String> result = new ArrayList<String>();
    if (LAST_CRAWL != null) {
      Iterator<SCustomObject> i = articleTypes.iterator();
      while (i.hasNext()) {
        SCustomObject customObject = i.next();
        result.addAll(removeArchivedAndOffline(FETCH_LIMIT, customObject, LAST_CRAWL));  
      }      
      result.addAll(removeDeleted(LAST_CRAWL));
    }	  
		return result;
	}

  /**
   * core update handler
   * @param limit - query limit
   * @param sObjectName - sObject being queried
   * @param metaFields - flat sObject definitions
   * @param childMetadataFields - child sObject definitions
   */
  private ArrayList<HashMap<String,String>> updateIndex(int limit, SCustomObject knowledgeArticleObj, ArrayList<String> childMetadataFields, Date lastCrawl) {
          
    ArrayList<HashMap<String, String>> results = new ArrayList<HashMap<String, String>>();
    
    ArrayList<String> metaFields = knowledgeArticleObj.getFieldsAsString();
    
    try {
      QueryResult queryResults = 
        connection.query(buildUpdateQuery(knowledgeArticleObj.getFullName(), limit, metaFields, childMetadataFields, lastCrawl));
      if (queryResults.getSize() > 0) {
        for (SObject s : queryResults.getRecords()) {
          HashMap<String, String> result = new HashMap<String,String>();
          // add to result attachment body if present
          addAttachmentBody(s, knowledgeArticleObj.getFullName(), result);
          
          // manually insert Article Type from the SObject
          result.put("ArticleType", knowledgeArticleObj.getName());
          
          // get fields that are flat
          for (int i=0; i<metaFields.size(); i++) {
            buildResult(s.getChild(metaFields.get(i)), result);
          }
          
          // get fields that contain n elements
          XmlObject categories = s.getChild(SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID);
          if (categories.hasChildren()) {   
            buildChildResult(categories, result);
          }
          
          // convert UserId to User Fullname
          addUserFullName(s, result);
          
          // fetch voting fields
          addVoteInfo(s, knowledgeArticleObj.getFullName(), result);
          results.add(result);
        }
      }
      
    } catch (ConnectionException ce) {
      UtilityLib.debugException(LOG, ce);
    }
    return results;
  }  
  
  /**
   * finds and deletes from index knowledgeArticles which should be removed
   * 
   * +articles that have been archived
   * +articles taken offline while editing
   * 
   * @param limit
   * @param sObjectName
   */
  private ArrayList<String> removeArchivedAndOffline(int limit, SCustomObject sObject, Date lastCrawl) {
    
    LOG.info("Salesforce Crawler:Querying for archived "+sObject.getFullName()+" objects...");
    
    int indexCt = 0;
      
    ArrayList<String> removeFromIndex = new ArrayList<String>();
    ArrayList<String> drafts = new ArrayList<String>();
    try {
      // 1. find archived articles, these should always be removed removed from index
      QueryResult archivedResults = 
        connection.query("SELECT "+SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID+" FROM "+sObject.getFullName()+
            " WHERE PublishStatus='Archived' AND Language = 'en_US' " +
            buildLastCrawlDateQuery(lastCrawl) +
            "ORDER BY LastModifiedDate DESC LIMIT "+limit);
      if (archivedResults.getSize() > 0) {
        for (SObject s : archivedResults.getRecords()) {
          if (s.getChild(SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID) != null && s.getChild(SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID).getValue() != null) {
            removeFromIndex.add(s.getChild(SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID).getValue().toString());
            indexCt++;
          }
        }
      }
      
      // 2a. find articles in Draft
      String d="SELECT "+SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID+" FROM "+sObject.getFullName()+
      " WHERE PublishStatus='Draft' AND Language = 'en_US'" +
      buildLastCrawlDateQuery(lastCrawl) +
      " LIMIT "+limit;
      LOG.info("Salesforce Crawler: all drafts d="+d);
      QueryResult draftResults =
        connection.query(d);
      if (draftResults.getSize() > 0) {
        for (SObject s : draftResults.getRecords()) {
          if (s.getChild(SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID) != null && s.getChild(SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID).getValue() != null) {
            drafts.add(s.getChild(SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID).getValue().toString());
          }
        }
      }
      // 2a. is the draft also Online?
      String q = "SELECT "+SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID+" FROM "+sObject.getFullName()+
          " WHERE PublishStatus='Online' AND Language = 'en_US'" +
          " AND KnowledgeArticleId IN ('"+UtilityLib.implodeArray(drafts, "','")+"') " +
          buildLastCrawlDateQuery(lastCrawl) +
          " LIMIT "+limit;
      QueryResult draftAndOnlineResults = 
        connection.query(q);
      if (draftAndOnlineResults.getSize() > 0) {
        for (SObject s : draftAndOnlineResults.getRecords()) {
          if (s.getChild(SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID) != null && s.getChild(SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID).getValue() != null) {
            // The article was found online, that means we should NOT remove from Index
            drafts.remove(s.getChild(SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID).getValue().toString());
          }
        }
      }
      // Combine archived and archivedANDNotOnline lists for removal from Index
      indexCt+=drafts.size();
      removeFromIndex.addAll(drafts);
      
    } catch (Exception e) {
      UtilityLib.debugException(LOG, e);
    }
    LOG.info("Salesforce Crawler:Removing "+indexCt+" "+sObject.getFullName()+" objects.");
    return removeFromIndex;
  }	
	
  /**
   * find deleted articles, removes them from index
   * 
   * @param limit
   */
  private ArrayList<String> removeDeleted(Date lastCrawl) {
    
    LOG.info("Salesforce Crawler:Querying for deleted objects...");
         
    ArrayList<String> removeFromIndex = new ArrayList<String>();
  
    String del = "SELECT "+SObjectField.FIELD_ID+" FROM KnowledgeArticle WHERE IsDeleted = true" +
        buildLastCrawlDateQuery(lastCrawl);
    try {
      QueryResult deleted = 
        connection.queryAll(del);
      if (deleted.getSize() > 0) {
        for (SObject s : deleted.getRecords()) {
          if (s.getChild(SObjectField.FIELD_ID) != null && s.getChild(SObjectField.FIELD_ID).getValue() != null) {
            removeFromIndex.add(s.getChild(SObjectField.FIELD_ID).getValue().toString());
          }
        }
      }
    } catch (ConnectionException ce) {
      UtilityLib.debugException(LOG, ce);
    }
    return removeFromIndex;
  }

  private void addVoteInfo(XmlObject parentObj, String sObjName, HashMap<String, String>result) {
  if (parentObj.hasChildren()) {
    String articleId = parentObj.getChild(SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID).getValue().toString();
    result.put(SObjectField.FIELD_VOTE_SCORE, fetchSfdcVoteInfo(articleId, sObjName));
    result.put(SObjectField.FIELD_VIEW_SCORE, fetchSfdcViewInfo(articleId, sObjName));
  }
  }
  
  private String fetchSfdcVoteInfo(String articleId, String sObjName) {
  String voteCount = "0";
  try {
    String q = "SELECT "+SObjectField.FIELD_NORMALIZED_SCORE+" FROM "+sObjName.substring(0, sObjName.length()-3)+"VoteStat WHERE Channel='AllChannels' AND IsDeleted=false AND ParentId = '"+articleId+"'";
    QueryResult queryResults = connection.query(q);
    if (queryResults.getSize() > 0) {
      for (SObject s : queryResults.getRecords()) {
        // grab the size element, that contains total votes
        if (s.hasChildren() && s.getChild(SObjectField.FIELD_NORMALIZED_SCORE) != null) {
          voteCount = s.getChild(SObjectField.FIELD_NORMALIZED_SCORE).getValue().toString();
        }
      }     
    }
  } catch (ConnectionException ce) {
    UtilityLib.debugException(LOG, ce);
  }
  return voteCount;
  }
  
  private String fetchSfdcViewInfo(String articleId, String sObjName) {
  String voteCount = "0";
  try {
    String q = "SELECT "+SObjectField.FIELD_NORMALIZED_SCORE+" FROM "+sObjName.substring(0, sObjName.length()-3)+"ViewStat WHERE Channel='AllChannels' AND IsDeleted=false AND ParentId = '"+articleId+"'";
    QueryResult queryResults = connection.query(q);
    if (queryResults.getSize() > 0) {
      for (SObject s : queryResults.getRecords()) {
        // grab the size element, that contains total votes
        if (s.hasChildren() && s.getChild(SObjectField.FIELD_NORMALIZED_SCORE) != null) {
          voteCount = s.getChild(SObjectField.FIELD_NORMALIZED_SCORE).getValue().toString();
        }
      }     
    }
  } catch (ConnectionException ce) {
    UtilityLib.debugException(LOG, ce);
  }
  return voteCount;
  }  
  /**
   * lookup the users full name based upon the userid from sfdc soql query
   * 
   * caches userId to make sure we limit number of queries to sfdc
   * @param sObj
   * @param result
   */
  private void addUserFullName(SObject sObj, HashMap<String, String>result) {
  if (sObj.hasChildren()) {
    // find fields that need mapping between userId and their full names
    for (int i=0; i<userIdToNameFields.size(); i++) {
      String userField = userIdToNameFields.get(i);
      XmlObject userIdObj = sObj.getChild(userField);
      if (userIdObj != null && UtilityLib.notEmpty(userIdObj.getValue().toString())) {
        // we have a valid userId, see if the mapping already exists
        String userId = userIdObj.getValue().toString();
        String userFullName = null;
        if (sfdcUserIdToUserFullname.containsKey(userId)) {
          userFullName = sfdcUserIdToUserFullname.get(userId);
        } else {
          // couldn't find it in our cache, lets query
          userFullName = fetchSfdcFullName(userId);
          if (userFullName != null) {
            // non empty result
            sfdcUserIdToUserFullname.put(userId, userFullName);
          }
        }
        // populate the final result with the actual name as opposed to an Id
        if (userFullName != null) {
          result.put(userField, userFullName);
        }
      }
    }
  }
  }
  
  private String fetchSfdcFullName(String userId) {
  String fullName = null;
  try {
    QueryResult queryResults = connection.query("SELECT Name FROM User WHERE id = '"+userId+"'");
    if (queryResults.getSize() > 0) {
      for (SObject s : queryResults.getRecords()) {
        if (s.hasChildren() && s.getChild("Name") != null) {
          fullName = s.getChild("Name").getValue().toString();
        }
      }
    }
  } catch (ConnectionException ce) {
    UtilityLib.debugException(LOG, ce);
  }
  return fullName;
  }
 
  /**
   * generates SOQL statement to just fetch attachment body
   * @param sObjectName
   * @param articleId
   * @return
   */
  private String buildAttachmentQuery(String sObjectName, String articleId) {
    String q = "SELECT "+SObjectField.FIELD_ATTACHMENT_BODY+" FROM "+sObjectName+
        " WHERE "+SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID+"='"+articleId+"'"+
        " AND PublishStatus = 'Online'";
    return q;
  }

  /**
   * genereates SOQL substatemnt when querying since last modified date
   * @return
   */
  private String buildLastCrawlDateQuery(Date lastCrawl) {
    String result = "";
    if (lastCrawl != null) {
      String lastMod = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ssZ").format(lastCrawl);
      result = " AND LastModifiedDate > "+lastMod;
    }
    return result;
  }
  
  

  /**
   * builds SOQL statement from designated sObject and its fields and sub-fields
   * 
   * @param sObjectName
   * @paam limit
   * @param metaFields - top level fields of sObject
   * @param childMetadataFields - sub fields of sObject
   */
  public String buildUpdateQuery(String sObjectName, int limit, ArrayList<String> metaFields, ArrayList<String> childMetadataFields, Date lastCrawl) {  
  
    String result = "SELECT ";
    result += UtilityLib.implodeArray(metaFields, ",");
    result += ", (SELECT "+ UtilityLib.implodeArray(childMetadataFields, ",") + " FROM DataCategorySelections ) ";
    result += " FROM "+sObjectName;
    result += " WHERE PublishStatus = 'Online' AND Language = 'en_US' AND IsDeleted = false";
    result += buildLastCrawlDateQuery(lastCrawl);
    result += " ORDER BY LastModifiedDate DESC LIMIT "+limit;
    return result;
  }
  
  /**
   * Determines if located sObject has attachments, queries sfdc for body, tika parses body
   * finally put processed text into result hash
   * @param sObj
   * @param sObjectName
   * @param result hash map where attachment body will be appended
   */
  private void addAttachmentBody(SObject sObj, String sObjectName, HashMap<String, String>result) {
    if (sObj.hasChildren()) {
      XmlObject len = sObj.getChild(SObjectField.FIELD_ATTACHMENT_LENGTH);
      if (len != null && len.getValue() != null) {
      String attachLenString = sObj.getChild(SObjectField.FIELD_ATTACHMENT_LENGTH).getValue().toString();
      float attachLength = Float.parseFloat(attachLenString);
      
      String articleId = sObj.getChild(SObjectField.FIELD_KNOWLEDGE_ARTICLE_ID).getValue().toString();
      if (UtilityLib.notEmpty(articleId) && attachLength > 0) {
        String contentType = sObj.getChild(SObjectField.FIELD_ATTACHMENT_CONTENT_TYPE).toString();
        // found a legit attachment file, query sfdc to get body
        try {
        QueryResult queryResults = connection.query(buildAttachmentQuery(sObjectName, articleId));
        if (queryResults.getSize() > 0) {
          for (SObject s : queryResults.getRecords()) {
            if (s.hasChildren()) {

              // run through tika
              Tika tika = new Tika();
                  Metadata metadata = new Metadata();
                  metadata.set(Metadata.CONTENT_TYPE, contentType);
              String body = s.getChild(SObjectField.FIELD_ATTACHMENT_BODY).getValue().toString();
                  
                  byte[] bits = DatatypeConverter.parseBase64Binary(body);
                  
                  ByteArrayInputStream bis = new ByteArrayInputStream(bits);
                  String attachString = tika.parseToString(bis, metadata);              
              // add body into result
              result.put("Attachment__Body_content", attachString);
            }
          }
        }
        } catch (ConnectionException ce) {
          UtilityLib.debugException(LOG, ce);
        } catch (IOException ie) {
          UtilityLib.debugException(LOG, ie);
        } catch (TikaException te) {
          UtilityLib.debugException(LOG, te);
        }
    }
    }
    }
  }
  
  /**
   * below classes should be moved to UpdateResult class
   */
  
  /**
   * adds into result hash fields from sub-sObjects like Category name/value
   * @param x
   * @param result
   */
  private void buildChildResult(XmlObject x, HashMap<String, String> result) {
    if (x.getChild(SObjectField.FIELD_DATACATEGORY_GROUP_NAME) != null && 
        x.getChild(SObjectField.FIELD_DATACATEGORY_GROUP_NAME).getValue() != null &&
        x.getChild(SObjectField.FIELD_DATACATEGORY_NAME) != null && 
        x.getChild(SObjectField.FIELD_DATACATEGORY_NAME).getValue() != null) {
      String key = x.getChild(SObjectField.FIELD_DATACATEGORY_GROUP_NAME).getValue().toString(); 
      if (result.containsKey(key)) {
        result.put(key, result.get(key)+","+x.getChild(SObjectField.FIELD_DATACATEGORY_NAME).getValue().toString());
      } else {
        result.put(key, x.getChild(SObjectField.FIELD_DATACATEGORY_NAME).getValue().toString());  
      }
    }
    if (x.hasChildren()) {
      Iterator<XmlObject> i = x.getChildren();
      while (i.hasNext()) {
        XmlObject o = i.next();
        if (o.getName().getLocalPart().equals("records")) {
          buildChildResult(o, result); 
        }
      }
    }
  }
  
  /**
   * adds into result hash top-level sObject fields
   * @param x
   * @param result
   */
  private void buildResult(XmlObject x, HashMap<String, String> result) {
    if (x != null && x.getValue() != null) {
      result.put(x.getName().getLocalPart(), x.getValue().toString());
    }
  }
  
  
  
	/**
	 * below classes should be moved to metadata classes
	 */

	  private void initializeMetaDataFields(String username, String password, String url) {
	    // only fetch metadata once at the start
	    if (LAST_CRAWL == null) {
  		  try {
    	    MetadataQueryEngine gen = new MetadataQueryEngine();
    	    articleTypes = gen.queryMetadata(username, password, url);
    	  } catch (Exception e) {
  		    UtilityLib.errorException(LOG, e);
  		  }
	    }
  		dataCategorySelectionsFields.add(SObjectField.FIELD_DATACATEGORY_GROUP_NAME);
  		dataCategorySelectionsFields.add(SObjectField.FIELD_DATACATEGORY_NAME);
  		
  		userIdToNameFields.add("OwnerId");
  		userIdToNameFields.add("CreatedById");
  		userIdToNameFields.add("LastModifiedById");
	  }

}
