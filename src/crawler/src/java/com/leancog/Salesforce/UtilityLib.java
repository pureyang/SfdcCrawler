package com.leancog.Salesforce;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;

import org.slf4j.Logger;

public class UtilityLib {
  
  public static boolean notEmpty(String s) {
		return (s != null && s.length() > 0);
	}

	public static String implodeArray(ArrayList<String> inputArray, String glueString) {
	
		String output = "";
		
		if (inputArray.size() > 0) {
			StringBuilder sb = new StringBuilder();
			sb.append(inputArray.get(0));
		
			for (int i=1; i<inputArray.size(); i++) {
				sb.append(glueString);
				sb.append(inputArray.get(i));
			}
		
			output = sb.toString();
		}

		return output;
	}
	
  public static void debugException(Logger log, Exception e) {
    log.debug(writeException(e).toString());
  }
 
  public static void infoException(Logger log, Exception e) {
    log.info(writeException(e).toString());
  }
 
  public static void warnException(Logger log, Exception e) {
    log.warn(writeException(e).toString());
  }
  
  public static void errorException(Logger log, Exception e) {
    log.error(writeException(e).toString());
  }   

  private static StringWriter writeException(Exception e) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    e.printStackTrace(pw);
    return sw;
  }
	  
}
