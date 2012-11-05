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
import com.lucid.crawl.CrawlState;
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
  
  CrawlState state;
  CrawlDataSource ds;
  long maxSize;
  int depth;
  boolean stopped = false;

  private static Date lastCrawl = null;
  private PartnerConnection connection;
  private String USERNAME = null;
  private String PASSWORD = null;
  
  private final int SFDC_FETCH_LIMIT = 1000;
  
  // TODO: faq and collateral fields should be fetched using sfdc metadata api 
  // contains fields for each article type
  private ArrayList<String> faqFields = new ArrayList<String>();
  private ArrayList<String> collateralFields = new ArrayList<String>(); 
  private ArrayList<String> dataCategorySelectionsFields = new ArrayList<String>(); 
  	  
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
        runSalesforceCrawl();
        
        // finished crawl, subsequent crawls only see modifications
        lastCrawl = new Date();
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
	faqFields.add("KnowledgeArticleId");
	faqFields.add("Title");
	faqFields.add("UrlName");
	faqFields.add("Answer__c");
	faqFields.add("FirstPublishedDate");
	faqFields.add("Question__c");
	faqFields.add("LastModifiedDate");
	faqFields.add("LastPublishedDate");
	faqFields.add("CreatedDate");
	faqFields.add("Summary");
	//faqFields.add("Attachment__Body__s");
	faqFields.add("Attachment__ContentType__s");
	faqFields.add("Attachment__Length__s");
	faqFields.add("Attachment__Name__s");

	collateralFields.add("KnowledgeArticleId");
	collateralFields.add("Title");
	collateralFields.add("Summary");
	collateralFields.add("OwnerId");
	collateralFields.add("UrlName");
	collateralFields.add("FirstPublishedDate");
	collateralFields.add("CreatedDate");
	collateralFields.add("CreatedById");
	collateralFields.add("LastModifiedById");
	collateralFields.add("LastModifiedDate");
	collateralFields.add("LastPublishedDate");
	// for some reason when including attachment body
	// only the latest collateral sObj gets fetched??
	//collateralFields.add("Attachment__Body__s");
	collateralFields.add("Attachment__ContentType__s");
	collateralFields.add("Attachment__Length__s");
	collateralFields.add("Attachment__Name__s");
	
	dataCategorySelectionsFields.add("DataCategoryGroupName");
	dataCategorySelectionsFields.add("DataCategoryName");
  }
  
  private void runSalesforceCrawl() throws Exception {
    LOG.info("Salesforce crawler started");
    
   	ConnectorConfig config = new ConnectorConfig();
    config.setUsername(USERNAME);
    config.setPassword(PASSWORD);
    config.setTraceMessage(true);
    
    try {
      connection = Connector.newConnection(config);
      
      LOG.info("Salesforce Crawler:Auth EndPoint: "+config.getAuthEndpoint());
      LOG.info("Salesforce Crawler:Service EndPoint: "+config.getServiceEndpoint());
      LOG.info("Salesforce Crawler:Username: "+config.getUsername());
      LOG.info("Salesforce Crawler:SessionId: "+config.getSessionId());
      
      // TODO: faq, collateral sObjects should be queried from sfdc
      updateIndex(SFDC_FETCH_LIMIT, "FAQ__kav", faqFields, dataCategorySelectionsFields);
      updateIndex(SFDC_FETCH_LIMIT, "Collateral__kav", collateralFields, dataCategorySelectionsFields);
      if (lastCrawl != null) {
    	  // only look to remove articles if this isn't first time crawling
    	  removeIndex(SFDC_FETCH_LIMIT, "FAQ__kav");
    	  removeIndex(SFDC_FETCH_LIMIT, "Collateral__kav");
    	  LOG.info("Salesforce crawler update since last crawl="+lastCrawl.toString());
      } else {
    	  LOG.info("Salesforce crawler INITIAL LOAD");
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
  private void updateIndex(int limit, String sObjectName, ArrayList<String> metaFields, ArrayList<String> childMetadataFields) {
    
    LOG.info("Salesforce Crawler: Querying for the "+limit+" newest "+sObjectName+"...");
    
    int indexCt = 0;
  	  
  	HashMap<String, String> result = new HashMap<String,String>();
    try {  	    
      QueryResult queryResults = 
    		connection.query(buildUpdateQuery(sObjectName, limit, metaFields, childMetadataFields));
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
  private void removeIndex(int limit, String sObjectName) {
	    
	    LOG.info("Salesforce Crawler: Querying for the "+limit+" archived "+sObjectName+"...");
	    
	    int indexCt = 0;
	  	  
	  	ArrayList<String> result=new ArrayList<String>();
	    try {  	    
	      QueryResult queryResults = 
	    		connection.query("SELECT KnowledgeArticleId FROM "+sObjectName+
	    				" WHERE PublishStatus='Archived' AND Language = 'en_US' " +
	    				buildLastCrawlDateQuery() +
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
	    LOG.info("Salesforce Crawler: Removing "+indexCt+" "+sObjectName+" objects.");
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
			
			LOG.info("Salesforce Crawler: indexing articleId="+articleId+" title="+title+" summary="+summary);
			
			// index articleType specific fields
			for (Entry<String, String> entry : values.entrySet()) {
				c.addMetadata(entry.getKey(), entry.getValue().toString());
				if (!entry.getKey().equals("Attachment__Body__s")) {
					LOG.info("Salesforce Crawler: indexing articleId="+articleId+" key="+entry.getKey()+" value="+entry.getValue().toString());
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
  private static void buildChildResult(XmlObject x, HashMap<String, String> result) {
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
  private static void buildResult(XmlObject x, HashMap<String, String> result) {
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
  public static String buildUpdateQuery(String sObjectName, int limit, ArrayList<String> metaFields, ArrayList<String> childMetadataFields) {  
	
  	String result = "SELECT ";
	result += implodeArray(metaFields, ",");
	result += ", (SELECT "+ implodeArray(childMetadataFields, ",") + " FROM DataCategorySelections ) ";
	result += " FROM "+sObjectName;
	result += " WHERE PublishStatus = 'Online' AND Language = 'en_US'";
	result += buildLastCrawlDateQuery();
	result += " ORDER BY LastModifiedDate DESC LIMIT "+limit;
	
	LOG.info("Salesforce Crawler: SOQL= "+result);
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
							LOG.info("Salesforce Crawler: articleId="+articleId+" found attachment body of type="+contentType);							
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
  
  /**
   * generates SOQL statement to just fetch attachment body
   * @param sObjectName
   * @param articleId
   * @return
   */
  private static String buildAttachmentQuery(String sObjectName, String articleId) {
		String q = "SELECT Attachment__Body__s FROM "+sObjectName+
				" WHERE KnowledgeArticleId='"+articleId+"'"+
				" AND PublishStatus = 'Online'";
		LOG.info("Salesforce Crawler: SOQL= "+q);
		return q;
  }

  /**
   * genereates SOQL substatemnt when quering since last modified date
   * @return
   */
  private static String buildLastCrawlDateQuery() {
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
	
	private static void handleException(Exception e) {
		StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		LOG.debug(sw.toString());
	}
}
