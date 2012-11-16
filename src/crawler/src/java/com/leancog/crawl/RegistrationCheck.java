package com.leancog.crawl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

import javax.crypto.Cipher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leancog.salesforce.UtilityLib;

public class RegistrationCheck {
  private static final Logger LOG = LoggerFactory.getLogger(RegistrationCheck.class);
  private static final String crawlerRegistrationFileName = "crawler.reg";
  private static final String distributedKeyFileName = "distributed.key";
  private final String ENCRYPTION_ALGORITHM = "RSA";
  
	public RegistrationCheck() {
		// upon creation, always check 
	}
	
	public boolean verify() {
		
	  // find path to serial file
	  
	  RandomAccessFile f;
    try {
      // read key file from jar
      File keyFile = new File(distributedKeyFileName);
      InputStream is = getClass().getClassLoader().getResourceAsStream(distributedKeyFileName);
      // convert InputStream to File
      OutputStream out = new FileOutputStream(keyFile);
      byte buf[]=new byte[1024];
      int len;
      while((len=is.read(buf))>0)
      out.write(buf,0,len);
      out.close();
      is.close();
      
      LOG.info("Salesforce Crawler:Looking for key file="+keyFile.getAbsolutePath());
      PrivateKey prvk = LoadEncryptionKey(keyFile, ENCRYPTION_ALGORITHM);

      // read the registration file for verification
      File parentFile = new File(crawlerRegistrationFileName);
      // hack to get to the right directory
      String reg = parentFile.getAbsolutePath().replace("bin", "crawlers");
      File registrationFile = new File(reg);
      LOG.info("Salesforce Crawler:Looking for registration file="+registrationFile.getAbsolutePath());
      
      f = new RandomAccessFile(registrationFile, "r");
      
      byte[] encBytesFromFile = new byte[(int)f.length()];
      f.readFully(encBytesFromFile);
      byte[] decBytes = decrypt(encBytesFromFile, prvk, ENCRYPTION_ALGORITHM);

      f.close();
      
      String decString = new String(decBytes);
      // plaintext is [companyname]::[date of registration]
      String[] tokens = decString.split("::");
      DateFormat dateformatter = new SimpleDateFormat("EEE MMM dd hh:mm:ss zzz yyyy");

      // check that registeredDate is within 3 months of right now
      // registrations are valid for 3 months
      Calendar nowDate = Calendar.getInstance();
      Calendar registeredCal = Calendar.getInstance();
      registeredCal.setTime(dateformatter.parse(tokens[1]));
      registeredCal.add(Calendar.MONTH, 3);
      
      if (nowDate.before(registeredCal)) {
        return true;
      }
      // cleanup registration file after completion
      keyFile.delete();
      LOG.warn("Salesforce Crawler:Registration expired on:"+registeredCal.getTime().toString());
    } catch (Exception e) {
      UtilityLib.errorException(LOG, e);
    }
	  
		// for now always return true
		return false;
	}
	
  private static byte[] decrypt(byte[] inpBytes, PrivateKey key,
      String xform) throws Exception{
    Cipher cipher = Cipher.getInstance(xform);
    cipher.init(Cipher.DECRYPT_MODE, key);
    return cipher.doFinal(inpBytes);
  }
  
  public PrivateKey LoadEncryptionKey(File keyFile, String algorithm) 
      throws IOException, NoSuchAlgorithmException,
      InvalidKeySpecException {
    // Read Private Key.
    File filePrivateKey = keyFile;
    FileInputStream fis = new FileInputStream(keyFile);
    byte[] encodedPrivateKey = new byte[(int) filePrivateKey.length()];
    fis.read(encodedPrivateKey);
    fis.close();

    // Generate PrivateKey
    KeyFactory keyFactory = KeyFactory.getInstance(algorithm);
    PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(
        encodedPrivateKey);
    PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);

    return privateKey;
    }
/*
  public PrivateKey LoadEncryptionKey(String algorithm) 
      throws IOException, NoSuchAlgorithmException,
      InvalidKeySpecException {
      // Read Private Key.
      InputStream is = this.getClass().getResourceAsStream("/"+distributedKeyFileName);
      long length = is.skip(Long.MAX_VALUE);
      byte[] encodedPrivateKey = new byte[(int)length];
      is = this.getClass().getResourceAsStream("/"+distributedKeyFileName);
      is.read(encodedPrivateKey);
      is.close();
  
      // Generate PrivateKey
      KeyFactory keyFactory = KeyFactory.getInstance(algorithm);
      PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(
          encodedPrivateKey);
      PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);
  
      return privateKey;
    }
*/  
}
