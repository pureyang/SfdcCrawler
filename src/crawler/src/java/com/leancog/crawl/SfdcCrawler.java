package com.leancog.crawl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import javax.xml.bind.DatatypeConverter;

import org.apache.tika.Tika;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lucid.Defaults.Group;
import com.lucid.admin.collection.datasource.DataSource;
import com.lucid.crawl.CrawlDataSource;
import com.lucid.crawl.CrawlStatus.JobState;
import com.lucid.crawl.io.Content;
import com.sforce.soap.partner.Connector;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
import com.sforce.ws.bind.XmlObject;
  
/**
 * Salesforce.com Crawler
 * @author Leancog
 */
public class SfdcCrawler implements Runnable {
 
  private static final Logger LOG = LoggerFactory.getLogger(SfdcCrawler.class);
  
  SfdcCrawlState state;
  CrawlDataSource ds;
  long maxSize;
  int depth;
  boolean stopped = false;

  private PartnerConnection connection;
  private String USERNAME = null;
  private String PASSWORD = null;
  private int SFDC_FETCH_LIMIT = 1000;
  // TOOD: this isn't the right place to put this date
  // there should be last crawl on each data source instance
  // @see com.lucid.crawl.History, com.lucid.crawl.DataSourceHistory
  private Date LAST_CRAWL = null;
  
  // TODO: faq and collateral fields should be fetched using sfdc metadata api 
  // contains fields for each article type
  private ArrayList<String> faqFields = new ArrayList<String>();
  private ArrayList<String> collateralFields = new ArrayList<String>(); 
  private ArrayList<String> dataCategorySelectionsFields = new ArrayList<String>();
  private ArrayList<String> userIdToNameFields = new ArrayList<String>();
  
  private HashMap<String, String> sfdcUserIdToUserFullname = null; 
  	  
  public SfdcCrawler(SfdcCrawlState state) {
    this.state = state;
    this.ds = (CrawlDataSource)state.getDataSource();
    maxSize = ds.getLong(DataSource.MAX_BYTES,
            DataSource.defaults.getInt(Group.datasource, DataSource.MAX_BYTES));
    depth = ds.getInt(DataSource.CRAWL_DEPTH,
            DataSource.defaults.getInt(Group.datasource, DataSource.CRAWL_DEPTH));
    if (depth < 1) {
      depth = Integer.MAX_VALUE;
    }
    // set sfdc API username and passwd
	USERNAME = ds.getString(SfdcSpec.SFDC_LOGIN);
	PASSWORD = ds.getString(DataSource.PASSWORD);
	
	SFDC_FETCH_LIMIT = ds.getInt(SfdcSpec.SFDC_MAX_ARTICLE_FETCH_COUNT, 1000);
	initializeMetaDataFields();
  }

  /*
   * Always use try/finally to ensure that the final state when finished is one of
   * the end states (finished, stopped, aborted).
   */
  @Override
  public void run() {
    // mark as starting
    state.getStatus().starting();
    try {
      state.getProcessor().start();
      if (ds.getType().equals("salesforce")) {
		// reset usermapping cache on every run
		sfdcUserIdToUserFullname = new HashMap<String, String>();

        // last crawl is on the state
        LAST_CRAWL = state.getLastCrawl();
        
        runSalesforceCrawl();
        

      }
    } catch (Throwable t) {
      LOG.warn("Exception in Salesforce crawl", t);
      state.getStatus().failed(t);
    } finally {
      boolean commit = ds.getBoolean(DataSource.COMMIT_ON_FINISH, true);
      try {
        state.getProcessor().finish();
        // optional commit - if false then it streamlines multiple small crawls
        state.getProcessor().getUpdateController().finish(commit);
      } catch (Exception e) {
        e.printStackTrace();
      }
      if (stopped) {
        state.getStatus().end(JobState.STOPPED);
      } else {
        state.getStatus().end(JobState.FINISHED);
      }
    }
  }
  
  public synchronized void stop() {
  	// lets clear the connection if crawler is stopped
    try {
      connection.logout();
    } catch (ConnectionException e1) {
    	StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e1.printStackTrace(pw);
      LOG.info(sw.toString());
    }    
    stopped = true;
    
  }
  
  private void initializeMetaDataFields() {
	  
	/*
	faqFields.add("KnowledgeArticleId");
	faqFields.add("Title");
	faqFields.add("Summary");
	faqFields.add("OwnerId");
	faqFields.add("UrlName");
	faqFields.add("ArticleNumber");
	faqFields.add("FirstPublishedDate");
	faqFields.add("LastModifiedById");
	faqFields.add("LastModifiedDate");
	faqFields.add("LastPublishedDate");
	faqFields.add("CreatedDate");
	faqFields.add("CreatedById");
	faqFields.add("IsVisibleInCsp");
	faqFields.add("IsVisibleInApp");
	faqFields.add("IsVisibleInPrm");
	faqFields.add("IsVisibleInPkb");
	*/
	  
	initDefaultFields(faqFields);
	// custom fields
	faqFields.add("Question__c");
	faqFields.add("Answer__c");
	faqFields.add("Attachment__ContentType__s");
	faqFields.add("Attachment__Length__s");
	faqFields.add("Attachment__Name__s");	

	initDefaultFields(collateralFields);

	// custom fields
	collateralFields.add("Attachment__ContentType__s");
	collateralFields.add("Attachment__Length__s");
	collateralFields.add("Attachment__Name__s");
	
	dataCategorySelectionsFields.add("DataCategoryGroupName");
	dataCategorySelectionsFields.add("DataCategoryName");
	
	userIdToNameFields.add("OwnerId");
	userIdToNameFields.add("CreatedById");
	userIdToNameFields.add("LastModifiedById");
  }
  
  private void initDefaultFields(ArrayList<String> target) {
	  target.add("KnowledgeArticleId");
	  target.add("Title");
	  target.add("Summary");
	  target.add("OwnerId");
	  target.add("UrlName");
	  target.add("ArticleNumber");
	  target.add("FirstPublishedDate");
	  target.add("LastModifiedById");
	  target.add("LastModifiedDate");
	  target.add("LastPublishedDate");
	  target.add("CreatedDate");
	  target.add("CreatedById");
	  target.add("IsVisibleInCsp");
	  target.add("IsVisibleInApp");
	  target.add("IsVisibleInPrm");
	  target.add("IsVisibleInPkb");
  }
  
  private void runSalesforceCrawl() throws Exception {
    LOG.info("Salesforce Crawler: Starting");
    
   	ConnectorConfig config = new ConnectorConfig();
    config.setUsername(USERNAME);
    LOG.info("Salesforce Crawler:Username: "+config.getUsername());
    config.setPassword(PASSWORD);
    //config.setTraceMessage(true);
    
    try {
      connection = Connector.newConnection(config);
      
      LOG.info("Salesforce Crawler:Auth EndPoint: "+config.getAuthEndpoint());
      LOG.info("Salesforce Crawler:Service EndPoint: "+config.getServiceEndpoint());
      
      
      // TODO: faq, collateral sObjects should be queried from sfdc
      updateIndex(SFDC_FETCH_LIMIT, "FAQ__kav", faqFields, dataCategorySelectionsFields, LAST_CRAWL);
      updateIndex(SFDC_FETCH_LIMIT, "Collateral__kav", collateralFields, dataCategorySelectionsFields, LAST_CRAWL);
      if (LAST_CRAWL != null) {
    	  // only look to remove articles if this isn't first time crawling
    	  removeIndex(SFDC_FETCH_LIMIT, "FAQ__kav", LAST_CRAWL);
    	  removeIndex(SFDC_FETCH_LIMIT, "Collateral__kav", LAST_CRAWL);
    	  LOG.info("Salesforce Crawler:Indexing since last crawl="+LAST_CRAWL.toString());
      } else {
    	  LOG.info("Salesforce Crawler:Indexing INITIAL LOAD");
      }
    } catch (ConnectionException ce) {
    	handleException(ce);
    }    
  }
  
  /**
   * core update handler
   * @param limit - query limit
   * @param sObjectName - sObject being queried
   * @param metaFields - flat sObject definitions
   * @param childMetadataFields - child sObject definitions
   */
  private void updateIndex(int limit, String sObjectName, ArrayList<String> metaFields, ArrayList<String> childMetadataFields, Date lastCrawl) {
       
    int indexCt = 0;
  	  
  	HashMap<String, String> result = new HashMap<String,String>();
    try {  	    
      QueryResult queryResults = 
    		connection.query(buildUpdateQuery(sObjectName, limit, metaFields, childMetadataFields, lastCrawl));
      if (queryResults.getSize() > 0) {
		  for (SObject s : queryResults.getRecords()) {
		  		
			// add to result attachment body if present
			addAttachmentBody(s, sObjectName, result);
			
			// index article type which is sObject type
			metaFields.add("type");
			
			// get fields that are flat
			for (int i=0; i<metaFields.size(); i++) {
				buildResult(s.getChild(metaFields.get(i)), result);
			}
			
			// get fields that contain n elements
			XmlObject categories = s.getChild("DataCategorySelections");
			if (categories.hasChildren()) {	  
				buildChildResult(categories, result);
			}
			
			// convert UserId to User Fullname
			addUserFullName(s, result);
			
			// fetch voting fields
			addVoteInfo(s, sObjectName, result);
			
			if (indexSObj(result)) {
				indexCt++;
			}
		  }
      }
      
    } catch (ConnectionException ce) {
    	handleException(ce);
    }
    LOG.info("Salesforce Crawler: Indexed "+indexCt+" "+sObjectName+" objects.");
  }  
  
  /**
   * finds and deletes from index knowledgeArticles which should be removed
   * 
   * +articles that have been archived
   * 
   * @param limit
   * @param sObjectName
   */
  private void removeIndex(int limit, String sObjectName, Date lastCrawl) {
	    
	    LOG.info("Salesforce Crawler:Querying for archived "+sObjectName+" objects...");
	    
	    int indexCt = 0;
	  	  
	  	ArrayList<String> result=new ArrayList<String>();
	    try {  	    
	      QueryResult queryResults = 
	    		connection.query("SELECT KnowledgeArticleId FROM "+sObjectName+
	    				" WHERE PublishStatus='Archived' AND Language = 'en_US' " +
	    				buildLastCrawlDateQuery(lastCrawl) +
	    				"ORDER BY LastModifiedDate DESC LIMIT "+limit);
	      if (queryResults.getSize() > 0) {
			  for (SObject s : queryResults.getRecords()) {
				  if (s.getChild("KnowledgeArticleId") != null && s.getChild("KnowledgeArticleId").getValue() != null) {
					  result.add(s.getChild("KnowledgeArticleId").getValue().toString());
					  indexCt++;
				  }
			  }
	      }
	      Iterator<String> i = result.iterator();
	      while (i.hasNext()) {
	    	  state.getProcessor().delete(i.next());
	      }
	    } catch (Exception e) {
	    	StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				e.printStackTrace(pw);
	      LOG.info(sw.toString());
	    }
	    LOG.info("Salesforce Crawler:Removing "+indexCt+" "+sObjectName+" objects.");
	  }  
  
  /**
   * takes result hash and indexes field/values into solr
   * @param values
   * @return
   */
  private boolean indexSObj(HashMap<String,String> values) {
	// calculate default solr fields, remove from result so we don't have duplicate solr fields
	String articleId = values.get("KnowledgeArticleId");
	String title = values.get("Title");
	values.remove("Title");
	String summary = values.get("Summary");
	values.remove("Summary");
	
	try {
		if (notEmpty(articleId) && notEmpty(title)) {
			// index sObject
			// default fields every index must have
			StringBuilder sb = new StringBuilder();
			Content c = new Content();
			c.setKey(articleId);
			sb.setLength(0);
			sb.append(summary);
			c.setData(sb.toString().getBytes());
			c.addMetadata("Content-Type", "text/html");
			c.addMetadata("title", title);
			
			LOG.debug("Salesforce Crawler: Indexing articleId="+articleId+" title="+title+" summary="+summary);
			
			// index articleType specific fields
			for (Entry<String, String> entry : values.entrySet()) {
				c.addMetadata(entry.getKey(), entry.getValue().toString());
				if (!entry.getKey().equals("Attachment__Body__s")) {
					LOG.debug("Salesforce Crawler: Indexing field key="+entry.getKey()+" value="+entry.getValue().toString());
				}
			}
			state.getProcessor().process(c);
			return true;
		}
    } catch (Exception e) {
    	handleException(e);
    }
	return false;
  }

  /**
   * adds into result hash fields from sub-sObjects like Category name/value
   * @param x
   * @param result
   */
  private void buildChildResult(XmlObject x, HashMap<String, String> result) {
	  if (x.getChild("DataCategoryGroupName") != null && 
			  x.getChild("DataCategoryGroupName").getValue() != null &&
			  x.getChild("DataCategoryName") != null && 
			  x.getChild("DataCategoryName").getValue() != null) {
		  String key = x.getChild("DataCategoryGroupName").getValue().toString(); 
		  if (result.containsKey(key)) {
			  result.put(key, result.get(key)+","+x.getChild("DataCategoryName").getValue().toString());
		  } else {
			  result.put(key, x.getChild("DataCategoryName").getValue().toString());  
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
   * builds SOQL statement from designated sObject and its fields and sub-fields
   * 
   * @param sObjectName
   * @paam limit
   * @param metaFields - top level fields of sObject
   * @param childMetadataFields - sub fields of sObject
   */
  public String buildUpdateQuery(String sObjectName, int limit, ArrayList<String> metaFields, ArrayList<String> childMetadataFields, Date lastCrawl) {  
	
  	String result = "SELECT ";
	result += implodeArray(metaFields, ",");
	result += ", (SELECT "+ implodeArray(childMetadataFields, ",") + " FROM DataCategorySelections ) ";
	result += " FROM "+sObjectName;
	result += " WHERE PublishStatus = 'Online' AND Language = 'en_US'";
	result += buildLastCrawlDateQuery(lastCrawl);
	result += " ORDER BY LastModifiedDate DESC LIMIT "+limit;
	
	return result;
  }
  
  /**
   * Determines if located sObject has attachments, queries sfdc for body, tika parses body
   * finally put processed text into result hash
   * @param sObj
   * @param sObjectName
   * @param result
   */
  private void addAttachmentBody(SObject sObj, String sObjectName, HashMap<String, String>result) {
		if (sObj.hasChildren()) {
			XmlObject len = sObj.getChild("Attachment__Length__s");
			if (len != null && len.getValue() != null) {
			String attachLenString = sObj.getChild("Attachment__Length__s").getValue().toString();
			float attachLength = Float.parseFloat(attachLenString);
			
			String articleId = sObj.getChild("KnowledgeArticleId").getValue().toString();
			if (notEmpty(articleId) && attachLength > 0) {
				String contentType = sObj.getChild("Attachment__ContentType__s").toString();
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
							String body = s.getChild("Attachment__Body__s").getValue().toString();
					        
					        byte[] bits = DatatypeConverter.parseBase64Binary(body);
					        
					        ByteArrayInputStream bis = new ByteArrayInputStream(bits);
					        String attachString = tika.parseToString(bis, metadata);							
							// add body into result
							result.put("Attachment__Body_content", attachString);
						}
					}
				}
				} catch (ConnectionException ce) {
					handleException(ce);
				} catch (IOException ie) {
					handleException(ie);
				} catch (TikaException te) {
					handleException(te);
				}
		}
		}
		}
  }
  
  private void addVoteInfo(XmlObject votes, String sObjName, HashMap<String, String>result) {
	if (votes.hasChildren()) {
		String articleId = votes.getChild("KnowledgeArticleId").getValue().toString();
		result.put("Votes", fetchSfdcVoteInfo(articleId, sObjName));
	}
  }
  
  private String fetchSfdcVoteInfo(String articleId, String sObjName) {
	String voteCount = "0";
	try {
		String q = "SELECT (SELECT Id FROM Votes) FROM "+sObjName.substring(0, sObjName.length()-1)+" WHERE id = '"+articleId+"'";
		QueryResult queryResults = connection.query(q);
		if (queryResults.getSize() > 0) {
			for (SObject s : queryResults.getRecords()) {
				// grab the size element, that contains total votes
				if (s.hasChildren() && s.getChild("Votes") != null && s.getChild("Votes").getChild("records") != null) {
					voteCount = s.getChild("Votes").getChild("size").getValue().toString();
				}
			}			
		}
	} catch (ConnectionException ce) {
		handleException(ce);
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
			if (userIdObj != null && notEmpty(userIdObj.getValue().toString())) {
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
		handleException(ce);
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
		String q = "SELECT Attachment__Body__s FROM "+sObjectName+
				" WHERE KnowledgeArticleId='"+articleId+"'"+
				" AND PublishStatus = 'Online'";
		return q;
  }

  /**
   * genereates SOQL substatemnt when quering since last modified date
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
  
  public static boolean notEmpty(String s) {
	return (s != null && s.length() > 0);
  }

  public static String implodeArray(ArrayList<String> inputArray, String glueString) {

	String output = "";
	
	if (inputArray.size() > 0) {
		StringBuilder sb = new StringBuilder();
		sb.append(inputArray.get(0));
	
		for (int i=1; i<inputArray.size(); i++) {
			sb.append(glueString);
			sb.append(inputArray.get(i));
		}
	
		output = sb.toString();
	}
	
	return output;
  }
	
  private void handleException(Exception e) {
	StringWriter sw = new StringWriter();
	PrintWriter pw = new PrintWriter(sw);
	e.printStackTrace(pw);
	LOG.debug(sw.toString());
  }
}
