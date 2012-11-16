package com.leancog.salesforce.metadata;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.leancog.salesforce.SCustomObject;
import com.leancog.salesforce.SObjectField;
import com.leancog.salesforce.UtilityLib;
import com.sforce.soap.metadata.AsyncRequestState;
import com.sforce.soap.metadata.AsyncResult;
import com.sforce.soap.metadata.MetadataConnection;
import com.sforce.soap.metadata.PackageTypeMembers;
import com.sforce.soap.metadata.RetrieveMessage;
import com.sforce.soap.metadata.RetrieveRequest;
import com.sforce.soap.metadata.RetrieveResult;

/**
 * Sample that logs in and shows a menu of retrieve and deploy metadata options.
 */ 
    
public class MetadataQueryEngine {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataQueryEngine.class);
  
  private static MetadataConnection metadataConnection;

  private static final String ARTICLES_ZIP_FILE = "articleTypes.zip";

  // manifest file that controls which components get retrieved 
  
  private static final String ARTICLE_TYPES_MANIFEST_FILE = "articleTypes.xml";

  private static final double API_VERSION = 26.0;

  // one second in milliseconds 
  private static final long ONE_SECOND = 1000;

  // maximum number of attempts to deploy the zip file 
  private static final int MAX_NUM_POLL_REQUESTS = 50;

  public MetadataQueryEngine() {
  }
  
  public ArrayList<SCustomObject> queryMetadata(String username, String password, String authPoint ) throws Exception {
    metadataConnection = MetadataLoginUtil.login(username, password, authPoint);
    // fetch article types zip
    File zip = retrieveZip(ARTICLE_TYPES_MANIFEST_FILE, ARTICLES_ZIP_FILE);
    
    // read in zip result, find all *__kav objects
    ArrayList<SCustomObject> articleTypesResult = findSObjectFieldsFromMetadata(ARTICLES_ZIP_FILE);
    
    zip.delete();
    return articleTypesResult;
  }
  
  private File retrieveZip(String manifestFile, String outputZip) throws Exception {
      RetrieveRequest retrieveRequest = new RetrieveRequest();
      retrieveRequest.setApiVersion(API_VERSION);
      setUnpackaged(manifestFile, retrieveRequest);

      AsyncResult asyncResult = metadataConnection.retrieve(retrieveRequest);
      asyncResult = waitForCompletion(asyncResult);
      RetrieveResult result =
          metadataConnection.checkRetrieveStatus(asyncResult.getId());

      // Print out any warning messages 
  
      StringBuilder stringBuilder = new StringBuilder();
      if (result.getMessages() != null) {
          for (RetrieveMessage rm : result.getMessages()) {
              stringBuilder.append(rm.getFileName() + " - " + rm.getProblem() + "\n");
          }
      }
      if (stringBuilder.length() > 0) {
          LOG.warn("Retrieve warnings:\n" + stringBuilder);
      }

      File resultsFile = new File(outputZip);
      FileOutputStream os = new FileOutputStream(resultsFile);

      try {
          os.write(result.getZipFile());
      } finally {
          os.close();
      }
      return resultsFile;
  }

  private AsyncResult waitForCompletion(AsyncResult asyncResult) throws Exception {
      int poll = 0;
      long waitTimeMilliSecs = ONE_SECOND;
      while (!asyncResult.isDone()) {
          Thread.sleep(waitTimeMilliSecs);
          // double the wait time for the next iteration 

          waitTimeMilliSecs *= 2;
          if (poll++ > MAX_NUM_POLL_REQUESTS) {
              throw new Exception(
                  "Request timed out. If this is a large set of metadata components, " +
                  "ensure that MAX_NUM_POLL_REQUESTS is sufficient.");
          }

          asyncResult = metadataConnection.checkStatus(
              new String[]{asyncResult.getId()})[0];
          System.out.println("Status is: " + asyncResult.getState());
      }

      if (asyncResult.getState() != AsyncRequestState.Completed) {
          throw new Exception(asyncResult.getStatusCode() + " msg: " +
              asyncResult.getMessage());
      }
      return asyncResult;
  }

  private void setUnpackaged(String manifestFile, RetrieveRequest request) throws Exception {

      // Note that we use the fully qualified class name because 
      // of a collision with the java.lang.Package class 
      com.sforce.soap.metadata.Package p = parsePackageManifest(manifestFile);
      request.setUnpackaged(p);
  }
  
  private ArrayList<SCustomObject> findSObjectFieldsFromMetadata(String filename) {
    ArrayList<SCustomObject> results = new ArrayList<SCustomObject>();
    try {
      FileInputStream fis = new FileInputStream(filename);
      ZipInputStream zis = new ZipInputStream(
              new BufferedInputStream(fis));
      ZipEntry entry;

      while ((entry = zis.getNextEntry()) != null) {
        if (entry.getName().contains("__kav")) {
          String objectOutputName = entry.getName().substring(entry.getName().lastIndexOf("/")+1, entry.getName().length());
          
          String customKnowledgeArticleObjectName = objectOutputName.substring(0, objectOutputName.indexOf("."));
          SCustomObject customSObject = new SCustomObject(customKnowledgeArticleObjectName);
          LOG.debug("Salesforce Crawler: Custom Knowledge Article Type found "+customKnowledgeArticleObjectName);
          int size;
          byte[] buffer = new byte[2048];

          FileOutputStream fos =
                  new FileOutputStream(objectOutputName);
          BufferedOutputStream bos =
                  new BufferedOutputStream(fos, buffer.length);

          while ((size = zis.read(buffer, 0, buffer.length)) != -1) {
              bos.write(buffer, 0, size);
          }
          bos.flush();
          bos.close();
          
          fos.close();
          LOG.debug("Salesforce Crawler: Wrote out metadata desc file "+objectOutputName);
          FileInputStream fisObj = new FileInputStream(objectOutputName);
          SAXReader reader = new SAXReader();   
          Document document = reader.read(fisObj);
          Element root = document.getRootElement();
          // iterate through child elements of root with element name "fields"
          for ( Iterator<Element> i = root.elementIterator("fields"); i.hasNext(); ) {
              Element fields = i.next();
              
              String FullName = null;
              String Label = null;
              String Type = null;
              for ( Iterator<Element> iField = fields.elementIterator(); iField.hasNext(); ) {
                Element field = iField.next();
                if (field.getName() == "fullName") {
                  FullName = field.getStringValue();
                }
                if (field.getName() == "label") {
                  Label = field.getStringValue();
                }
                if (field.getName() == "type") {
                  Type = field.getStringValue();
                }
              }
              LOG.debug("Salesforce Crawler: Field found fullname="+FullName+" label="+Label+" type="+Type);
              //Add fields to custom object
              customSObject.addField(new SObjectField(FullName, Label, Type));
              File metaFile = new File(objectOutputName);
              // clean up after use
              metaFile.delete();
          }
          results.add(customSObject);
          fisObj.close();
        }
      }
      zis.close();
      fis.close();
    } catch (IOException e) {
      UtilityLib.errorException(LOG, e);
    } catch (DocumentException de) {
      UtilityLib.errorException(LOG, de);
    }
    LOG.debug("Salesforce Crawler: Metadata API found "+results.size() +" custom objects");
    return results;
  }
  

  private com.sforce.soap.metadata.Package parsePackageManifest(String file)
          throws ParserConfigurationException, IOException, SAXException {
    com.sforce.soap.metadata.Package packageManifest = null;
    List<PackageTypeMembers> listPackageTypes = new ArrayList<PackageTypeMembers>();
    DocumentBuilder db =
            DocumentBuilderFactory.newInstance().newDocumentBuilder();
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream(file);
    
    org.w3c.dom.Element d = db.parse(inputStream).getDocumentElement();
    for (org.w3c.dom.Node c = d.getFirstChild(); c != null; c = c.getNextSibling()) {
        if (c instanceof org.w3c.dom.Element) {
          org.w3c.dom.Element ce = (org.w3c.dom.Element) c;
          org.w3c.dom.NodeList nodeList = ce.getElementsByTagName("name");
            if (nodeList.getLength() == 0) {
                continue;
            }
            String name = nodeList.item(0).getTextContent();
            org.w3c.dom.NodeList m = ce.getElementsByTagName("members");
            List<String> members = new ArrayList<String>();
            for (int i = 0; i < m.getLength(); i++) {
              org.w3c.dom.Node mm = m.item(i);
                members.add(mm.getTextContent());
            }
            PackageTypeMembers packageTypes = new PackageTypeMembers();
            packageTypes.setName(name);
            packageTypes.setMembers(members.toArray(new String[members.size()]));
            listPackageTypes.add(packageTypes);
        }
    }
    packageManifest = new com.sforce.soap.metadata.Package();
    PackageTypeMembers[] packageTypesArray =
            new PackageTypeMembers[listPackageTypes.size()];
    packageManifest.setTypes(listPackageTypes.toArray(packageTypesArray));
    packageManifest.setVersion(API_VERSION + "");
    return packageManifest;
  }
}
