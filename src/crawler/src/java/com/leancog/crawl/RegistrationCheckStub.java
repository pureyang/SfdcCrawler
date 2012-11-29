package com.leancog.crawl;

/**
 * bypasses registration check for sfdc-crawler 
 * used for testing/code review
 * @author leancog
 *
 */
public class RegistrationCheckStub {
  

	public RegistrationCheckStub() {
	}
	
	public boolean verify() {
		// always return true
		return true;
	}

}
