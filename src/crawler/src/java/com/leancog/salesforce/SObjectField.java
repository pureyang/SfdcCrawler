package com.leancog.salesforce;

/**
 * represents one field on a Salesforce SObject
 * @author leancog
 *
 */

public class SObjectField {
  
  // Salesforce fields
  public static final String FIELD_KNOWLEDGE_ARTICLE_ID = "KnowledgeArticleId";
  public static final String FIELD_ID = "Id";
  public static final String FIELD_DATACATEGORY_SELECTIONS = "DataCategorySelections";
  public static final String FIELD_DATACATEGORY_GROUP_NAME ="DataCategoryGroupName";
  public static final String FIELD_DATACATEGORY_NAME = "DataCategoryName";
  public static final String FIELD_VOTE_SCORE = "VoteScore";
  public static final String FIELD_VIEW_SCORE = "ViewScore";
  public static final String FIELD_NORMALIZED_SCORE = "NormalizedScore";
  public static final String FIELD_ATTACHMENT_LENGTH = "Attachment__Length__s";
  public static final String FIELD_ATTACHMENT_CONTENT_TYPE = "Attachment__ContentType__s";
  public static final String FIELD_ATTACHMENT_BODY = "Attachment__Body__s";
  
  // Salesforce Meta fields
  public static final String META_FULL_NAME = "fullName";
  public static final String META_LABEL = "label";
  public static final String META_TYPE = "type";
      
  private String FullName;
  private String Label;
  private String Type;
  
  public SObjectField(String fullName, String label, String type) {
    this.FullName = fullName;
    this.Label = label;
    this.Type = type;
  }
  
  public String getType() {
    return Type;
  }
  public void setType(String type) {
    Type = type;
  }
  public String getLabel() {
    return Label;
  }
  public void setLabel(String label) {
    Label = label;
  }
  
  public String getFullName() {
    return FullName;
  }
  public void setFullName(String name) {
    FullName = name;
  }

  
}
