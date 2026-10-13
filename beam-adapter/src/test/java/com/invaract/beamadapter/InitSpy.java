package com.invaract.beamadapter;

/**
 * A class that is not a Beam runner and records, in a system property, whether anything initialised it: naming it as the
 * delegate must be refused without running its static initialiser. (The record is a property, not a field, because reading a
 * static field would itself initialise the class.)
 */
public class InitSpy {
  public static final String PROPERTY = "invaract.initspy.initialised";

  static {
    System.setProperty(PROPERTY, "true");
  }
}
