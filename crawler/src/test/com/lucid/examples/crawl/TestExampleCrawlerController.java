package com.lucid.examples.crawl;

import java.io.File;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.junit.Test;

import com.lucid.LWECrawlerTestBase;
import com.lucid.admin.collection.datasource.DataSource;
import com.lucid.crawl.CrawlDataSource;
import com.lucid.crawl.CrawlId;
import com.lucid.crawl.CrawlStatus;
import com.lucid.crawl.CrawlerController;
import com.lucid.crawl.CrawlerUtils;
import com.lucid.utils.FileUtilsTest;

public class TestExampleCrawlerController extends LWECrawlerTestBase {
  
  private static final String CC_EX = "lucid.example";
  
  /*
   * Init our crawler. Only the built-in standard crawlers are initialized
   * automatically.
   */
  @Override
  public void setUp() throws Exception {
    super.setUp();
    crawlerControllerRegistry.initCrawler(CC_EX, ExampleCrawlerController.class);
  }
  
  @Test
  public void testFileCrawl() throws Exception {
    File dir = extractTestFiles("crawler/testWebCrawl1.zip");
    // Find out how many files we expect to process
    int numSourceTestFiles = FileUtilsTest.countFiles(dir);
    numSourceTestFiles += 2; // Tika sub-documents
    log.info("testLocalFS: numSourceTestFiles =  " + numSourceTestFiles);

    // Construct the parameters, request, and response for a new job
    String crawlUrl = dir.getCanonicalPath();
    CrawlDataSource ds = new CrawlDataSource("xfile", CC_EX, collection1);
    ds.setPath(crawlUrl);
    ds.setDisplayName("file crawl");
    ds.setProperty(DataSource.COMMIT_ON_FINISH, true);
    cm.addDataSource(collection1, ds);
    Map<String,Object> status = crawlerManager.crawl(ds);

    Long id = (Long) status.get(DataSource.ID);
    CrawlerController cc = crawlerControllerRegistry.get(CC_EX);
    CrawlerUtils.waitJob(cc, new CrawlId(id));
    Thread.sleep(2000);

    status = crawlerManager.getStatus(cc, new CrawlId(id), ds.getDataSourceId());
    Long failedDocs = (Long) status.get(CrawlStatus.NUM_FAILED);
    assertNotNull(failedDocs);
    assertEquals(0l, failedDocs.longValue());

    // See what is actually in the index
    dumpDocIds("indexed docs");
    Map<String, Document> docs = getIndexedDocs();
    int numIndexedDocs = docs.size();
    assertEquals(numSourceTestFiles, numIndexedDocs);
  }
  
  @Test
  public void testRandomCrawl() throws Exception {
    CrawlDataSource ds = new CrawlDataSource("xrandom", CC_EX, collection1);
    ds.setProperty(XRandomSpec.NUM_DOCS, 100);
    ds.setDisplayName("random crawl");
    ds.setProperty(DataSource.COMMIT_ON_FINISH, true);
    cm.addDataSource(collection1, ds);
    Map<String,Object> status = crawlerManager.crawl(ds);

    Long id = (Long) status.get(DataSource.ID);
    CrawlerController cc = crawlerControllerRegistry.get(CC_EX);
    CrawlerUtils.waitJob(cc, new CrawlId(id));
    Thread.sleep(2000);

    status = crawlerManager.getStatus(cc, new CrawlId(id), ds.getDataSourceId());
    Long failedDocs = (Long) status.get(CrawlStatus.NUM_FAILED);
    assertNotNull(failedDocs);
    assertEquals(0l, failedDocs.longValue());
    Long newDocs = (Long) status.get(CrawlStatus.NUM_NEW);
    assertTrue(newDocs.longValue() > 0 && newDocs.longValue() <= 100L);
  }

}
