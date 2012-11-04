package com.leancog.crawl;

import com.lucid.Defaults.Group;
import com.lucid.admin.collection.datasource.DataSource;
import com.lucid.crawl.DataSourceSpec;
import com.lucid.spec.SpecProperty;
import com.lucid.spec.Validator;

/**
 * Specification for SfdcDataSource properties. This specification is
 * used to supply default values and determines the validation.
 * 
 */
public class SfdcSpec extends DataSourceSpec {
  public static final String SFDC_LOGIN = "sfdc_login";
  
  public SfdcSpec() {
    super(Category.FileSystem.toString());
  }

  @Override
  protected void addCrawlerSupportedProperties() {
    addSpecProperty(new SpecProperty(SFDC_LOGIN,
            "Salesforce API Username", 
            String.class,
            null,
            Validator.NOT_BLANK_VALIDATOR, 
            true));
  
    // Salesforce API Password + Security Token
    addSpecProperty(new SpecProperty(DataSource.PASSWORD,
            DS_+DataSource.PASSWORD, 
            String.class,
            DataSource.defaults.getString(Group.datasource, DataSource.PASSWORD),
            Validator.NOT_BLANK_VALIDATOR, 
            true));

    // this source supports batch processing options
    addBatchProcessingProperties();
    // this source supports field mapping options
    addFieldMappingProperties();
    // this source supports boundary limits (exclude/include/bounds) options
    addBoundaryLimitsProperties();
    // this source supports reachability check during DS creation
    addVerifyAccessProperties();
    // this source support commit-related options
    addCommitProperties();
  }

}
