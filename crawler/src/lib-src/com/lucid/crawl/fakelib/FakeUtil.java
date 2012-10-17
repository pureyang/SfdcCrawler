package com.lucid.crawl.fakelib;

import java.util.Random;

/**
 * This is a trivial utility class that exists only to illustrate that nested
 * jars and classes that they contain are available in the crawler's
 * classloader. 
 */
public class FakeUtil {
  private static Random r = new Random();
  
  public static String randomValue() {
    return "Random value: " + r.nextInt();
  }
  
  public static int nextInt(int maxValue) {
    return r.nextInt(maxValue);
  }

}
