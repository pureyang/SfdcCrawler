package com.leancog.crawl;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

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
    LOG.info("Sfdc crawler started");
    
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
      
      removeIndex(SFDC_FETCH_LIMIT, "FAQ__kav");
      removeIndex(SFDC_FETCH_LIMIT, "Collateral__kav");
      
    } catch (ConnectionException e1) {
    	StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e1.printStackTrace(pw);
      LOG.info(sw.toString());
    }    
  }
  
  /**
   * 
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
    		connection.query(buildSOQLQuery(sObjectName, limit, metaFields, childMetadataFields));
      if (queryResults.getSize() > 0) {
		  for (SObject s : queryResults.getRecords()) {
		    // TODO: post-process attachment using tika
			// lets not put the raw attachment into result if we can avoid it.
			  
			
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
      
    } catch (Exception e) {
    	StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
      LOG.info(sw.toString());
    }
    LOG.info("Salesforce Crawler: Indexed "+indexCt+" "+sObjectName+" objects.");
  }  
  
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
   * 
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
			// default fields
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
    	StringWriter sw = new StringWriter();
		PrintWriter pw = new PrintWriter(sw);
		e.printStackTrace(pw);
		LOG.warn(sw.toString());
    }
	return false;
  }

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
  
  private static void buildResult(XmlObject x, HashMap<String, String> result) {
	  if (x != null && x.getValue() != null) {
		  result.put(x.getName().getLocalPart(), x.getValue().toString());
	  }
  }
  public static String buildSOQLQuery(String sObjectName, int limit, ArrayList<String> metaFields, ArrayList<String> childMetadataFields) {  

	
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

  // keeping these methods around in case we end up downloading attachments from 
  // sfdc during crawl
  /*
   * Traverse the file system hierarchy up to a depth.
   *
  private void traverse(File f, int curDepth) {
    if (curDepth > depth || stopped) {
      return;
    }
    if (f.isDirectory()) {
      File[] files = f.listFiles();
      for (File file : files) {
        traverse(file, curDepth + 1);
      }
    } else {
      if (!f.canRead()) {
        state.getStatus().incrementCounter(Counter.Failed);
        return;
      }
      // retrieve the content
      Content c = getContent(f);
      if (c == null) {
        state.getStatus().incrementCounter(Counter.Failed);
        return;
      }
      // this should increment counters as needed
      try {
        state.getProcessor().process(c);
      } catch (Exception e) {
        state.getStatus().incrementCounter(Counter.Failed);
      }
    }
  } */
  
  /*
   * Retrieve the content of the file + some repository metadata
   *
  private Content getContent(File f) {
    if (f.length() > maxSize) {
      return null;
    }
    try {
      Content c = new Content();
      // set this to a unique identifier
      c.setKey(f.getAbsolutePath());
      StringBuilder sb = new StringBuilder();
      Date date = new Date(f.lastModified());
      try {
        DateUtil.formatDate(date, null, sb);
      } catch (IOException ioe) {
        sb.setLength(0);
        sb.append(date.toString());
      }
      c.addMetadata("Last-Modified", sb.toString());
      c.addMetadata("Content-Length", String.valueOf(f.length()));
      byte[] data = new byte[(int)f.length()];
      // for simplicity we don't check for IO errors...
      FileInputStream fis = new FileInputStream(f);
      fis.read(data);
      fis.close();
      c.setData(data);
      return c;
    } catch (Exception e) {
      return null;
    }
  }  */
}
