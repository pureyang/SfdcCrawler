package com.leancog.crawl;

import java.util.Date;

import com.lucid.crawl.CrawlState;

public class SfdcCrawlState extends CrawlState {
  SfdcCrawler crawler = null;
  Date lastCrawl = null;

  Thread t = null;
  
  public void start() throws Exception {
    if (t != null && t.isAlive()) {
      throw new Exception("already running");
    }
    crawler = new SfdcCrawler(this);
    t = new Thread(crawler);
    t.setDaemon(true);
    t.start();
  }
  
  public void stop() throws Exception {
    if (t == null || !t.isAlive()) {
      throw new Exception("not running");
    }
    crawler.stop();
  }
  
  public Date getLastCrawl() {
	return lastCrawl;
  }

  public void setLastCrawl(Date lastCrawl) {
	this.lastCrawl = lastCrawl;
  }
}
