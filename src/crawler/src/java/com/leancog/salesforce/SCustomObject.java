package com.leancog.salesforce;


import java.util.ArrayList;
import java.util.Iterator;

import com.leancog.salesforce.metadata.DefaultSObjectFields;

/**
 * SCustomObject represents one SalesForce SObject and all of its fields
 * @author leancog
 *
 */
public class SCustomObject {
  
  private final String FullName;
  private final String Name;
  private ArrayList<SObjectField> fields;


  public ArrayList<SObjectField> getFields() {
    return fields;
  }
 
  public void addField(SObjectField field) {
    if (field.getFullName().equals("Attachment__c") &&
        field.getType().equals("File")) {
      // special behavior when attachment field detected, convert to the actual metadata required for query
      this.fields.add(new SObjectField("Attachment__ContentType__s", "ContentType", "text"));
      this.fields.add(new SObjectField("Attachment__Length__s", "ContentLength", "text"));
      this.fields.add(new SObjectField("Attachment__Name__s", "ContentName", "text"));
    } else {
      this.fields.add(field);
    }
  }

  /**
   * full API SObject name, e.g "Collateral__kav"
   * @return
   */
  public String getFullName() {
    return FullName;
  }

  /**
   * logical SObject name, e.g. Collateral
   * @return
   */
  public String getName() {
    return Name;
  }

  public SCustomObject(String name) {
    this.FullName = name;
    this.Name = this.FullName.substring(0, name.indexOf("__kav"));
    this.fields = DefaultSObjectFields.initDefaultFields();
    
  }
  
  /**
   * hold over method until we can fully use fields
   * @return
   */
  public ArrayList<String> getFieldsAsString() {
    ArrayList<String> result = new ArrayList<String>();
    Iterator<SObjectField> fields = this.fields.iterator();
    while(fields.hasNext()) {
      String name =fields.next().getFullName(); 
      result.add(name);
    }
    return result;
  }

}
