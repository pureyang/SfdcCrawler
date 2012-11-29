package com.leancog.salesforce.metadata;

import java.util.ArrayList;

import com.leancog.salesforce.SObjectField;

public class DefaultSObjectFields {
  /**
   * initializes default sfdc obj fields
   * @author leancog
   */
    public static ArrayList<SObjectField> initDefaultFields() {
      ArrayList<SObjectField> target = new ArrayList<SObjectField>();
      target.add(new SObjectField("KnowledgeArticleId", "KnowledgeArticleId", "text"));
      target.add(new SObjectField("Title", "Title", "text"));
      target.add(new SObjectField("Summary", "Summary", "text"));
      target.add(new SObjectField("OwnerId", "OwnerId", "userId"));
      target.add(new SObjectField("UrlName", "UrlName", "text"));
      target.add(new SObjectField("ArticleNumber", "ArticleNumber", "text"));
      target.add(new SObjectField("FirstPublishedDate", "FirstPublishedDate", "text"));
      target.add(new SObjectField("LastModifiedById", "LastModifiedById", "userId"));

      
      target.add(new SObjectField("LastModifiedDate", "LastModifiedDate", "text"));
      target.add(new SObjectField("LastPublishedDate", "LastPublishedDate", "text"));
      target.add(new SObjectField("CreatedDate", "CreatedDate", "text"));
      target.add(new SObjectField("CreatedById", "CreatedById", "userId"));
      target.add(new SObjectField("IsVisibleInCsp", "IsVisibleInCsp", "text"));
      target.add(new SObjectField("IsVisibleInApp", "IsVisibleInApp", "text"));
      target.add(new SObjectField("IsVisibleInPrm", "IsVisibleInPrm", "text"));
      target.add(new SObjectField("IsVisibleInPkb", "IsVisibleInPkb", "text"));      

      return target;
    }
}
