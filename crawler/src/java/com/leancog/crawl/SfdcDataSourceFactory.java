package com.leancog.crawl;

import java.util.Map;

import com.lucid.admin.collection.datasource.DataSource;
import com.lucid.admin.collection.datasource.FieldMapping;
import com.lucid.admin.collection.datasource.FieldMappingUtil;
import com.lucid.crawl.CrawlDataSource;
import com.lucid.crawl.CrawlerController;
import com.lucid.crawl.DataSourceFactory;
import com.lucid.crawl.DataSourceFactoryException;

/**
 * SfdcKnowledge data source factory responsible for reporting the list
 * of supported data source types and for the creation of data source instances
 * from a map of parameters. Most of this happens in {@link DataSourceFactory},
 * here we just need to initialize the list of supported types and their
 * specifications.
 */
public class SfdcDataSourceFactory extends DataSourceFactory {

  public SfdcDataSourceFactory(CrawlerController cc) {
    super(cc);
    // map type names to specifications
    types.put("sfdcknowledge", new SfdcKnowledgeSpec());
    // map names to implementation classes
    impls.put("sfdcknowledge", CrawlDataSource.class);
    // everything else (instantiation, validation, etc) is
    // handled by the magic of DataSourceFactory
  }

  /**
   * Override this method to perform some other data source initialization.
   * Here we initialize the field mapping properties with some default mappings.
   */
  @Override
  public DataSource create(Map<String, Object> m, String collection)
          throws DataSourceFactoryException {
    // TODO Auto-generated method stub
    DataSource ds = super.create(m, collection);
    Map<String,Object> mapping = (Map<String,Object>)m.get(DataSource.FIELD_MAPPING);
    if (mapping != null && !mapping.isEmpty()) {
      // deserialize & set as overrides
      FieldMapping.fromMap(ds.getFieldMapping(), mapping);
    }
    // overlay with the default Tika mappings
    FieldMappingUtil.addTikaFieldMapping(ds.getFieldMapping(), ds.getCollection(), false);
    return ds;
  }

}
