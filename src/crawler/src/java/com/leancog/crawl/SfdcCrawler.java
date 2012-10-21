package com.leancog.crawl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

import org.apache.solr.common.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lucid.Defaults.Group;
import com.lucid.admin.collection.datasource.DataSource;
import com.lucid.crawl.CrawlDataSource;
import com.lucid.crawl.CrawlState;
import com.lucid.crawl.CrawlStatus.Counter;
import com.lucid.crawl.CrawlStatus.JobState;
import com.lucid.crawl.io.Content;

import com.sforce.soap.enterprise.Connector;
import com.sforce.soap.enterprise.DeleteResult;
import com.sforce.soap.enterprise.EnterpriseConnection;
import com.sforce.soap.enterprise.Error;
import com.sforce.soap.enterprise.QueryResult;
import com.sforce.soap.enterprise.SaveResult;
import com.sforce.soap.enterprise.sobject.FAQ__kav;
import com.sforce.soap.enterprise.sobject.Collateral__kav;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;
  
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

	private EnterpriseConnection connection;
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
    PASSWORD = ds.getString(SfdcSpec.SFDC_PASSWD)+ds.getString(SfdcSpec.SFDC_SECURITY_TOKEN);
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
      
      // TODO refactor the below into external lib
      // TODO how do we dynamically figure out the type of Knowledge Articles?
      queryIndexFAQ(1000, "FAQ__kav");
      queryIndexCollateral(1000, "Collateral__kav");
      
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
    
    try {  
    	// TODO how to get this list of SObject fields dynamically?	    
      QueryResult queryResults = connection.query("SELECT KnowledgeArticleId, Title, VersionNumber, Answer__c, LastModifiedDate " +
      		"FROM "+sObjectName+" WHERE PublishStatus = 'Online' AND Language = 'en_US' ORDER BY LastModifiedDate DESC LIMIT "+limit);
      if (queryResults.getSize() > 0) {
        for (int i=0;i<queryResults.getRecords().length;i++) {
          // cast the SObject to a strongly-typed FAQ
          FAQ__kav faq = (FAQ__kav)queryResults.getRecords()[i];
          
          
          LOG.debug("KnowledgeArticleId: " + faq.getKnowledgeArticleId() + " - Title: "+faq.getTitle()+
    	    " - LastModifiedDate: "+faq.getLastModifiedDate().getTime().toString()
  	      + " - VersionNumber: "+faq.getVersionNumber()
	        + " - Answer: "+faq.getAnswer__c());
        	
        	if (indexSObj(faq.getKnowledgeArticleId(), faq.getTitle(), faq.getAnswer__c())) {
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
  
  // TODO refactor this into a factory pattern based on article type
  private void queryIndexCollateral(int limit, String sObjectName) {
    
    LOG.info("Salesforce Crawler: Querying for the "+limit+" newest "+sObjectName+"...");
    
    int indexCt = 0;
    
    try {  
    	// TODO how to get this list of SObject fields dynamically?
      QueryResult queryResults = connection.query("SELECT KnowledgeArticleId, Title, VersionNumber, Summary, LastModifiedDate " +
      		"FROM "+sObjectName+" WHERE PublishStatus = 'Online' AND Language = 'en_US' ORDER BY LastModifiedDate DESC LIMIT "+limit);
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
  
  // TODO refactor this into a factory pattern based on articleType
  // TODO what additional meta data should be set?
  // TODO how to attach attachments
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
	
	// TODO move this to a leancog library
	public static boolean notEmpty(String s) {
		return (s != null && s.length() > 0);
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
