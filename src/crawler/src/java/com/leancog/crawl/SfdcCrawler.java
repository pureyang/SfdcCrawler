package com.leancog.crawl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.solr.common.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lucid.Defaults.Group;
import com.lucid.admin.collection.datasource.DataSource;
import com.lucid.crawl.CrawlDataSource;
import com.lucid.crawl.CrawlState;
import com.lucid.crawl.CrawlStatus.Counter;
import com.lucid.crawl.CrawlStatus.JobState;
import com.lucid.crawl.fakelib.FakeUtil;
import com.lucid.crawl.io.Content;

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
    stopped = true;
  }
  
  private void runSalesforceCrawl() throws Exception {
    LOG.info("Sfdc crawler started");
    
	// just put something into the index
    StringBuilder sb = new StringBuilder();
	Content c = new Content();
    c.setKey("sfdc");
    sb.setLength(0);
    // insert the time that the crawler was run
    DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	Date date = new Date();
    sb.append(" SFDC Crawler was here " + dateFormat.format(date));
    c.setData(sb.toString().getBytes());
    c.addMetadata("Content-Type", "text/plain");
    c.addMetadata("title", "SFDC Crawler");
    state.getProcessor().process(c);
  }

  // keeping these methods around in case we end up downloading attachments from sfdc during crawl
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
      // Note: FakeUtil just illustrates that nested jars are available from
      // the crawler's classloader
      c.addMetadata("Random-Value", FakeUtil.randomValue());
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
