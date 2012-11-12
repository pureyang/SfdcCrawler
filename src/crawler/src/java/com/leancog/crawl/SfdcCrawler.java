package com.leancog.crawl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leancog.Salesforce.PartnerQueryEngine;
import com.leancog.Salesforce.UpdateQueryResult;
import com.leancog.Salesforce.UtilityLib;
import com.lucid.Defaults.Group;
import com.lucid.admin.collection.datasource.DataSource;
import com.lucid.crawl.CrawlDataSource;
import com.lucid.crawl.CrawlStatus.Counter;
import com.lucid.crawl.CrawlStatus.JobState;
import com.lucid.crawl.io.Content;
import com.sforce.ws.ConnectionException;
  
/**
 * Salesforce.com Crawler
 * @author Leancog
 */
public class SfdcCrawler implements Runnable {
 
  private static final Logger LOG = LoggerFactory.getLogger(SfdcCrawler.class);
  private static final String REGISTRATION_ERROR_MSG = "Salesforce Crawler: Crawl Stopped, Failed Registration check.";
  
  SfdcCrawlState state;
  CrawlDataSource ds;
  long maxSize;
  int depth;
  boolean stopped = false;

  private PartnerQueryEngine partnerQueryEngine = null;
  private int SFDC_FETCH_LIMIT = 1000;
  	  
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
    // initialize PQE
    partnerQueryEngine = new PartnerQueryEngine();
	
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
        // leancog registration check
      	RegistrationCheck checker = new RegistrationCheck();
      	if (!checker.verify()) {
      		Exception failed = new Exception(REGISTRATION_ERROR_MSG);
      		UtilityLib.errorException(LOG, failed);
      		state.getStatus().failed(failed);
      		return;
      	}
        
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
    	partnerQueryEngine.logout();
    } catch (ConnectionException e1) {
    	UtilityLib.warnException(LOG, e1);
    }    
    stopped = true;
  }
  
  
  private void runSalesforceCrawl() throws Exception {
    LOG.info("Salesforce Crawler: Starting");
    
    try {
      partnerQueryEngine.initConnection(
          ds.getString(SfdcSpec.SFDC_LOGIN), 
          ds.getString(DataSource.PASSWORD), 
          ds.getInt(SfdcSpec.SFDC_MAX_ARTICLE_FETCH_COUNT, SFDC_FETCH_LIMIT),
          state.getLastCrawl()
        );
    } catch (ConnectionException ce) {
      LOG.error("Salesforce Crawler:Failed to connect to salesforce. Stopping Crawl.");
      UtilityLib.errorException(LOG, ce);
      state.getStatus().failed(ce);
      return;
    }

    UpdateQueryResult updateResults = partnerQueryEngine.findUpdates();
    indexUpdates(updateResults);
    
    indexDeleted(partnerQueryEngine.findDeleteds());
  }
  
  

  private void indexDeleted(ArrayList<String> removeFromIndex) {
    // remove fromIndex
    try {
      Iterator<String> i = removeFromIndex.iterator();
      while (i.hasNext()) {
        String id = i.next().toString();
        LOG.info("Salesforce Crawler: Removing deleted article KnowledgeArticleId="+id);
        state.getProcessor().delete(id);
      }
    } catch (Exception e) {
      UtilityLib.debugException(LOG, e);
      state.getStatus().incrementCounter(Counter.Failed);
    }
    
  
  }  
  /**
   * takes result hash and indexes field/values into solr
   * @param values
   * @return
   */
  private void indexUpdates(UpdateQueryResult updates) {
    Iterator<HashMap<String, String>> i = updates.getIterator();
    while (i.hasNext()) {
      HashMap<String, String> values = i.next();
  
    	// during index default solr fields are indexed separately
    	String articleId = values.get("KnowledgeArticleId");
    	String title = values.get("Title");
    	values.remove("Title");
    	String summary = values.get("Summary");
    	values.remove("Summary");
    	
    	try {
    		if (UtilityLib.notEmpty(articleId) && UtilityLib.notEmpty(title)) {
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
    		}
      } catch (Exception e) {
      	UtilityLib.debugException(LOG, e);
      	state.getStatus().incrementCounter(Counter.Failed);
      }
    }
  }

}
