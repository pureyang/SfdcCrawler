package com.leancog.crawl;

import com.leancog.crawl.SfdcSpec;
import java.io.PrintWriter;
import java.io.StringWriter;
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
 * A simple crawler that can handle data source types supported by this
 * crawler controller.
 */
public class SfdcCrawler implements Runnable {
 
  private static final Logger LOG = LoggerFactory.getLogger(SfdcCrawler.class);
  
  CrawlState state;
  CrawlDataSource ds;
  long maxSize;
  int depth;
  boolean stopped = false;

  private PartnerConnection connection;
  private String USERNAME = null;
  private String PASSWORD = null;
  
  public SfdcCrawler(CrawlState state) {
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
    //PASSWORD = ds.getString(SfdcSpec.SFDC_PASSWD)+ds.getString(SfdcSpec.SFDC_SECURITY_TOKEN);
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
  
  private void runSalesforceCrawl() throws Exception {
    LOG.info("Sfdc crawler started");
    
   	ConnectorConfig config = new ConnectorConfig();
    config.setUsername(USERNAME);
    config.setPassword(PASSWORD);
    config.setTraceMessage(true);
    
    try {
      connection = Connector.newConnection(config);
      
      // display some current settings
      LOG.info("Salesforce Crawler:Auth EndPoint: "+config.getAuthEndpoint());
      LOG.info("Salesforce Crawler:Service EndPoint: "+config.getServiceEndpoint());
      LOG.info("Salesforce Crawler:Username: "+config.getUsername());
      LOG.info("Salesforce Crawler:SessionId: "+config.getSessionId());
      
      // TODO refactor the below with call to sfdc custom setting via APEX API call
      queryIndexFAQ(1000, "FAQ__kav");
      // using partner wsdl only on faq for now
      //queryIndexCollateral(1000, "Collateral__kav");
      
    } catch (ConnectionException e1) {
    	StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e1.printStackTrace(pw);
      LOG.info(sw.toString());
    }    
  }
  
  // TODO refactor this into a factory pattern based on article type
  private void queryIndexFAQ(int limit, String sObjectName) {
    
    LOG.info("Salesforce Crawler: Querying for the "+limit+" newest "+sObjectName+"...");
    
    int indexCt = 0;
    
  	String[] faqFields = 
	  	{"KnowledgeArticleId",
	  	"Title",
	  	"UrlName",
	  	"Answer__c",
	  	"LastModifiedDate",
	  	"FirstPublishedDate",
	  	"Question__c",
	  	"LastModifiedDate",
	  	"LastPublishedDate",
	  	"CreatedDate",
	  	"Summary",
	  	"SystemModstamp",
	  	"Title",
	  	"UrlName",
	  	"Attachment__Body__s",
	  	"Attachment__ContentType__s",
	  	"Attachment__Length__s",
	  	"Attachment__Name__s",
	  	"type"};
  	
  	String[] dataCategorySelectionsFields = 
	  	{"DataCategoryGroupName",
	  	"DataCategoryName"};    
  	HashMap<String, String> result = new HashMap<String,String>();
    try {  
    	// TODO how to get this list of SObject fields dynamically?	    
      QueryResult queryResults = 
    		connection.query("SELECT Answer__c, ArchivedDate, Attachment__Body__s, Attachment__ContentType__s, Attachment__Length__s, Attachment__Name__s, CreatedDate, FirstPublishedDate, IsDeleted, KnowledgeArticleId, LastModifiedDate, LastPublishedDate, Question__c, Summary, Title, UrlName," +
    				"(SELECT DataCategoryGroupName,DataCategoryName FROM DataCategorySelections)" +
    		 "FROM "+sObjectName+" WHERE PublishStatus = 'Online' AND Language = 'en_US' ORDER BY LastModifiedDate DESC LIMIT "+limit);
      if (queryResults.getSize() > 0) {
		  for (SObject s : queryResults.getRecords()) {
			

			// get fields that are flat
			for (int i=0; i<faqFields.length; i++) {
				buildResult(s.getChild(faqFields[i]), result);
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
    LOG.info("Salesforce Crawler: Indexing "+indexCt+" "+sObjectName+" objects.");
  }  
  
  private boolean indexSObj(HashMap<String,String> values) {
	  String articleId = values.get("KnowledgeArticleId");
	  String title = values.get("Title");
	  String answer = values.get("Answer__c");
	  // build input xml, not sure how to use this yet
	  String body = "<update><doc>";
	  for (Entry<String, String> entry : values.entrySet()) {
		    String key = entry.getKey();
		    Object value = entry.getValue();
		    body += "<field name=\""+key+"\">"+value+"</field>";
	  }
	  body += "</doc></update>";
	  // add into Content
	try {
		if (notEmpty(articleId) && notEmpty(title)) {
			// put the FAQ into the index after a basic sanity check
			StringBuilder sb = new StringBuilder();
			Content c = new Content();
			c.setKey(articleId);
			sb.setLength(0);
			sb.append(answer);
			c.setData(sb.toString().getBytes());
			c.addMetadata("Content-Type", "text/html");
			for (Entry<String, String> entry : values.entrySet()) {
				c.addMetadata(entry.getKey(), entry.getValue().toString());
			}
			c.addMetadata("title", title);
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
 
  public String buildCollateralQuery(String sObjectName, int limit) {
	// TODO get list of SObject fields dynamically from metadata API	  
  	String[] collateralFields = 
	  	{"ArchivedDate",
	  	"Attachment__Body__s",
	  	"Attachment__ContentType__s",
	  	"Attachment__Length__s",
	  	"Attachment__Name__s",
	  	"CreatedById",
	  	"CreatedDate",
	  	"FirstPublishedDate",
	  	"IsDeleted",
	  	"KnowledgeArticleId",
	  	"LastModifiedById",
	  	"LastModifiedDate",
	  	"LastPublishedDate",
	  	"OwnerId",
	  	"Summary",
	  	"SystemModstamp",
	  	"Title",
	  	"UrlName",
	  	"IsVisibleInCsp",
	  	"IsVisibleInApp",
	  	"IsVisibleInPrm",
	  	"IsVisibleInPkb"};

  	String[] dataCategorySelectionsFields = 
	  	{"DataCategoryGroupName",
	  	"DataCategoryName"}; 	
  	
  	String result = "SELECT ";
  	result += implodeArray(collateralFields, ",");
  	result += ", (SELECT "+ implodeArray(dataCategorySelectionsFields, ",") + " FROM DataCategorySelections ) ";
  	result += " FROM "+sObjectName+" WHERE PublishStatus = 'Online' AND Language = 'en_US' ORDER BY LastModifiedDate DESC LIMIT "+limit;
	return result;
  }
  
	// TODO below this to a leancog library
	public static boolean notEmpty(String s) {
		return (s != null && s.length() > 0);
	}
	
	public static String implodeArray(String[] inputArray, String glueString) {

		String output = "";

		if (inputArray.length > 0) {
			StringBuilder sb = new StringBuilder();
			sb.append(inputArray[0]);

			for (int i=1; i<inputArray.length; i++) {
				sb.append(glueString);
				sb.append(inputArray[i]);
			}

			output = sb.toString();
		}

		return output;
	}
	// TODO END below this to a leancog library
	
	
	  // TODO refactor this into a factory pattern based on article type
	/*
	  private void queryIndexCollateral(int limit, String sObjectName) {
	    
	    LOG.info("Salesforce Crawler: Querying for the "+limit+" newest "+sObjectName+"...");
	    
	    int indexCt = 0;
	    
	    try { 
	      QueryResult queryResults = connection.query(buildCollateralQuery(sObjectName, limit));
	      if (queryResults.getSize() > 0) {
	        for (int i=0;i<queryResults.getRecords().length;i++) {
	          // cast the SObject to a strongly-typed Collateral
	          // TODO this is the key to refactoring into a factory
	          Collateral__kav col = (Collateral__kav)queryResults.getRecords()[i];
	          
	          
	          LOG.debug("KnowledgeArticleId: " + col.getKnowledgeArticleId() + " - Title: "+col.getTitle()+
	    	    " - LastModifiedDate: "+col.getLastModifiedDate().getTime().toString()
	  	      + " - VersionNumber: "+col.getVersionNumber()
		        + " - Summary: "+col.getSummary());
	        	
	        	if (indexSObj(col.getKnowledgeArticleId(), col.getTitle(), col.getSummary())) {
		        	indexCt++;
		        }
	        }
	      }
	      
	    } catch (Exception e) {
	    	StringWriter sw = new StringWriter();
				PrintWriter pw = new PrintWriter(sw);
				e.printStackTrace(pw);
	      LOG.error(sw.toString());
	    }
	    LOG.info("Salesforce Crawler: Indexing "+indexCt+" "+sObjectName+" objects.");
	  } 
	  */
	  // TODO refactor this into a factory pattern based on articleType
	  // TODO what additional meta data should be set?
	  // TODO how to attach attachments
/* old index method used for enterprise wsdl
	  private boolean indexSObj(String articleId, String title, String body) { 
		try {
				if (notEmpty(articleId) &&
					notEmpty(title)) {
					// put the FAQ into the index after a basic sanity check
					StringBuilder sb = new StringBuilder();
					Content c = new Content();
					c.setKey(articleId);
					sb.setLength(0);
					sb.append(body);
					c.setData(sb.toString().getBytes());
					c.addMetadata("Content-Type", "text/html");
					c.addMetadata("title", title);
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
*/	  
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
