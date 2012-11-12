package com.leancog.crawl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.crypto.Cipher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.leancog.Salesforce.UtilityLib;

public class RegistrationCheck {
  private static final Logger LOG = LoggerFactory.getLogger(RegistrationCheck.class);
  private static final String crawlerRegistrationFileName = "crawler.reg";
  private final String xform = "RSA";
  
	public RegistrationCheck() {
		// upon creation, always check 
	}
	
	public boolean verify() {
		
	  // find path to serial file
	  //ClassLoader classLoader = RegistrationCheck.class.getClassLoader();
	  //File classpathRoot = new File(classLoader.getResource("").getPath());

	  RandomAccessFile f;
    try {
      // read registration file
      PrivateKey prvk = LoadEncryptionKey(".", xform);

      // read the registration file for verification
      f = new RandomAccessFile(new File("./"+crawlerRegistrationFileName), "r");  
      
      byte[] encBytesFromFile = new byte[(int)f.length()];
      f.readFully(encBytesFromFile);
      byte[] decBytes = decrypt(encBytesFromFile, prvk, xform);

      f.close();
      
      String decString = new String(decBytes);
      
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
  
  public PrivateKey LoadEncryptionKey(String path, String algorithm) 
      throws IOException, NoSuchAlgorithmException,
      InvalidKeySpecException {
      // Read Private Key.
      File filePrivateKey = new File(path + "/distributed.key");
      LOG.info("Salesforce Crawler:Looking for key file="+filePrivateKey.getAbsolutePath());
      FileInputStream fis = new FileInputStream(path + "/distributed.key");
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

}
