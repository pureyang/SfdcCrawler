package com.leancog.salesforce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

public class UpdateQueryResult {
  private ArrayList<HashMap<String,String>> result;
  
  public UpdateQueryResult() {
    result = new ArrayList<HashMap<String,String>>();
  }
  
  public void addResults(ArrayList<HashMap<String,String>> results) {
    Iterator<HashMap<String, String>> i = results.iterator();
    while (i.hasNext()) {
      result.add(i.next());
    }
  }
  
  public Iterator<HashMap<String, String>> getIterator() {
    return result.iterator();
  }
  
  
  

}
