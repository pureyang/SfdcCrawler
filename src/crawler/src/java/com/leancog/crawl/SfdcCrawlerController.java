package com.leancog.crawl;

import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lucid.admin.collection.datasource.DataSource;
import com.lucid.admin.collection.datasource.DataSourceId;
import com.lucid.crawl.CrawlId;
import com.lucid.crawl.CrawlProcessor;
import com.lucid.crawl.CrawlerController;
import com.lucid.crawl.DataSourceFactory;
import com.lucid.crawl.batch.BatchManager;

public class SfdcCrawlerController extends CrawlerController {
  private static final Logger LOG = LoggerFactory.getLogger(SfdcCrawlerController.class);
  private static Date LAST_CRAWL = null;

  /** Symbolic name to use when initializing and using REST API. */
  public static final String CC_EX = "leancog.crawler";
  
  private DataSourceFactory dsFactory;
  private String uuid;

  public SfdcCrawlerController() {
    batchMgr = BatchManager.create(CC_EX, this.getClass().getClassLoader());
    dsFactory = new SfdcDataSourceFactory(this);
  }
  
  @Override
  public DataSourceFactory getDataSourceFactory() {
    return dsFactory;
  }

  @Override
  public BatchManager getBatchManager() {
    return batchMgr;
  }

  @Override
  public void reset(String collection, DataSourceId dsId) throws Exception {
    // not supported - no persistent crawl state
  }

  @Override
  public void resetAll(String collection) throws Exception {
    // not supported - no persistent crawl state
  }

  @Override
  public CrawlId defineJob(DataSource ds, CrawlProcessor processor)
          throws Exception {
    // for simplicity we implement a 1:1 correspondence between jobs and data sources
    CrawlId cid = new CrawlId(ds.getDataSourceId());
    SfdcCrawlState state = new SfdcCrawlState();
    state.init(ds, processor, historyRecorder);
    jobStateMgr.add(state);
    return cid;
  }

  /** Jobs must be running asynchronously. In this case our subclass of CrawlState
   * starts the processing thread.
   */
  @Override
  public void startJob(CrawlId jobId) throws Exception {
    SfdcCrawlState state = (SfdcCrawlState)jobStateMgr.get(jobId);
    if (state == null) {
      throw new Exception("unknown crawl id " + jobId);
    }
    // if this is the first crawl, then lastcrawl is null
    state.setLastCrawl(LAST_CRAWL);
    if (state.getStatus().isRunning()) {
      LOG.warn("already running");
      return;
    }
    state.start();
    LAST_CRAWL = new Date();
  }

  @Override
  public void stopJob(CrawlId jobId) throws Exception {
    SfdcCrawlState state = (SfdcCrawlState)jobStateMgr.get(jobId);
    if (state == null) {
      throw new Exception("unknown crawl id " + jobId);
    }
    if (!state.getStatus().isRunning()) {
      LOG.warn("not running");
      return;
    }
    state.stop();
  }

  @Override
  public void abortJob(CrawlId id) throws Exception {
    stopJob(id);
  }

}
