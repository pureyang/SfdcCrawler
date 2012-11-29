package com.leancog.salesforce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import net.sf.json.JSONObject;
import net.sf.json.JSONSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author leancog
 *
 */
public class UtilityLib {
  private static final Logger LOG = LoggerFactory.getLogger(UtilityLib.class);
  private static final String SOLR_API_PORT = "8888"; // default LWE solr http port
  
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
  
  
  /**
   * attempts to fetch last crawl date of the crawlers datasource, if not found returns null
   * @param dsName
   * @param dsId
   * @return
   */
  public static Date getLastCrawl(String dsName, long dsId) {
    Date last = null;
    // the below assumes default installation 
    String solrConnection = "http://localhost:"+SOLR_API_PORT+"/api/collections/"+dsName+"/datasources/"+dsId+"/status";
    LOG.error("salesforce crawler: connection string = "+solrConnection);
    String jsonResponse = getJSON(solrConnection);
    if (jsonResponse == null) {
      return last;
    }    
    
    JSONObject jo = (JSONObject) JSONSerializer.toJSON( jsonResponse);
    String crawlStarted= jo.getString( "crawl_started" );
    if (crawlStarted == "null") {
      return null;
    } else {
      String pattern = "yyyy-MM-dd'T'HH:mm:ssZ";
      SimpleDateFormat format = new SimpleDateFormat(pattern);
      try {
        last = format.parse(crawlStarted);
      } catch (ParseException e) {
        LOG.warn("Salesforce crawler: received invalid date on field last_crawl  from solr API, defaulting to null last crawl", e);
      }
    }
    return last;
  }
  
  private static String getJSON(String url) {
    try {
        URL u = new URL(url);
        HttpURLConnection c = (HttpURLConnection) u.openConnection();
        c.setRequestMethod("GET");
        c.setRequestProperty("Content-length", "0");
        c.setUseCaches(false);
        c.setAllowUserInteraction(false);
        c.connect();
        int status = c.getResponseCode();

        switch (status) {
            case 200:
            case 201:
                BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream()));
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line+"\n");
                }
                br.close();
                return sb.toString();
        }

    } catch (MalformedURLException ex) {
      LOG.warn("Salesforce crawler: invalid solr API url="+url, ex);
    } catch (IOException ex) {
      LOG.warn("Salesforce crawler: unable to connect to solr API url="+url, ex);
    }
    return null;
}
  
	  
}
