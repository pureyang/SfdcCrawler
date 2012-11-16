package com.leancog.salesforce;

public class SObjectField {
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
