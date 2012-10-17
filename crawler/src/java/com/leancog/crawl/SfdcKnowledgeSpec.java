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
 * Specification for XFileDataSource properties. This specification is
 * used to supply default values and determines the validation.
 */
public class SfdcKnowledgeSpec extends DataSourceSpec {
  public static final String NUM_DOCS = "num_docs";

  public static final String SFDC_LOGIN = "sfdc_login";
  public static final String SFDC_PASSWD = "sfdc_passwd";
  public static final String SFDC_SEUCURITY_TOKEN = "sfdc_security_token";
  public SfdcKnowledgeSpec() {
    super(Category.FileSystem.toString());
  }

  @Override
  protected void addCrawlerSupportedProperties() {
    // we require a non-negative integer number (provided as a string or int)
    addSpecProperty(new SpecProperty(NUM_DOCS,
            "max number of generated documents", Integer.class,
            0, new NumDocsValidator(), true));          
            
    addSpecProperty(new SpecProperty(SFDC_LOGIN,
            "Salesforce API Username", String.class,
            null, Validator.NOT_BLANK_VALIDATOR, true));
    addSpecProperty(new SpecProperty(SFDC_PASSWD,
            "Salesforce API Password", String.class,
            null, Validator.NOT_BLANK_VALIDATOR, true));   
    addSpecProperty(new SpecProperty(SFDC_SEUCURITY_TOKEN,
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

  private static class NumDocsValidator extends NonNegIntStringValidator {

    @Override
    protected List<Error> checkInt(int parseInt, SpecProperty confProp) {
      List<Error> errors = super.checkInt(parseInt, confProp);
      if (errors != null && !errors.isEmpty()) {
        return errors;
      }
      if (parseInt < 1) {
        return Collections.singletonList(
                new Error(confProp.name, E_INVALID_VALUE, "value must be > 0, was " + parseInt));
      }
      return null;
    }
  }
}
