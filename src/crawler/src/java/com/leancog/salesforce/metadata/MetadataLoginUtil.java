package com.leancog.salesforce.metadata;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.soap.partner.LoginResult;
import com.sforce.soap.metadata.MetadataConnection;
import com.sforce.ws.ConnectionException;
import com.sforce.ws.ConnectorConfig;

/**
 * Login utility.
 */ 
    
public class MetadataLoginUtil {

  public static MetadataConnection login(String UserName, String Password, String Url) throws ConnectionException {
    final LoginResult loginResult = loginToSalesforce(UserName, Password, Url);
    return createMetadataConnection(loginResult);
  }

  private static MetadataConnection createMetadataConnection(
    final LoginResult loginResult) throws ConnectionException {
    final ConnectorConfig config = new ConnectorConfig();
    config.setServiceEndpoint(loginResult.getMetadataServerUrl());
    config.setSessionId(loginResult.getSessionId());
    return new MetadataConnection(config);
  }

  private static LoginResult loginToSalesforce(
          final String username,
          final String password,
          final String loginUrl) throws ConnectionException {
    final ConnectorConfig config = new ConnectorConfig();
    config.setAuthEndpoint(loginUrl);
    config.setServiceEndpoint(loginUrl);
    config.setManualLogin(true);
    return (new PartnerConnection(config)).login(username, password);
  }
}