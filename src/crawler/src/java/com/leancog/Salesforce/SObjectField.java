package com.leancog.Salesforce;

public class SObjectField {
	private String Name;
	private SObjectType Type;
	
	public String getName() {
		return Name;
	}
	public void setName(String name) {
		Name = name;
	}
	public SObjectType getType() {
		return Type;
	}
	public void setType(SObjectType type) {
		Type = type;
	}
	
}
