package com.leancog.crawl;

import com.lucid.crawl.CrawlState;

public class SfdcCrawlState extends CrawlState {
  SfdcCrawler crawler = null;

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
  
}
