package com.leancog.crawl;

import static com.lucid.api.Error.E_INVALID_VALUE;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.lucid.admin.collection.CollectionManager;
import com.lucid.admin.collection.datasource.DataSource;
import com.lucid.api.Error;
import com.lucid.crawl.CrawlerUtils;
import com.lucid.crawl.DataSourceSpec;
import com.lucid.spec.SpecProperty;
import com.lucid.spec.Validator;
import com.lucid.spec.NonNegIntStringValidator;
import com.lucid.utils.StringUtils;

/**
 * Specification for SfdcDataSource properties. This specification is
 * used to supply default values and determines the validation.
 */
public class SfdcSpec extends DataSourceSpec {

  public static final String SFDC_API_VER = "sfdc_api_ver";
  public static final String SFDC_API_ENV = "sfdc_api_env";
  public static final String SFDC_LOGIN = "sfdc_login";
  public static final String SFDC_PASSWD = "sfdc_passwd";
  public static final String SFDC_SECURITY_TOKEN = "sfdc_security_token";  
  public SfdcSpec() {
    super(Category.FileSystem.toString());
  }

  @Override
  protected void addCrawlerSupportedProperties() {
    addSpecProperty(new SpecProperty(SFDC_API_VER,
            "Salesforce API Version", String.class,
            "26.0", Validator.NOT_BLANK_VALIDATOR, true));
            
    addSpecProperty(new SpecProperty(SFDC_API_ENV,
            "Salesforce Environment (test/prod)", String.class,
            "test", Validator.NOT_BLANK_VALIDATOR, true));
            
    addSpecProperty(new SpecProperty(SFDC_LOGIN,
            "Salesforce API Username", String.class,
            null, Validator.NOT_BLANK_VALIDATOR, true));
            
    addSpecProperty(new SpecProperty(SFDC_PASSWD,
            "Salesforce API Password", String.class,
            null, Validator.NOT_BLANK_VALIDATOR, true));
             
    addSpecProperty(new SpecProperty(SFDC_SECURITY_TOKEN,
            "Salesforce API Security Token", String.class,
            null, Validator.NOT_BLANK_VALIDATOR, true));                 
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
