package com.leancog.Salesforce;


import java.util.ArrayList;
import java.util.Iterator;

import com.leancog.Salesforce.metadata.DefaultSObjectFields;

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

  public String getFullName() {
    return FullName;
  }

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
