/*
 * Copyright 2024 Block Inc.
 */

package com.squareup.perfutils;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * Utilities for writing log messages.
 */
public class LoggingUtils {
  /**
   * Extract the stack trace from a Throwable.
   *
   * @param t The throwable from which we wish to extract the stack trace.
   * @return A string representation of the input's stack trace.
   */
  public static String getStackTrace(Throwable t) {
    StringWriter sw = new StringWriter();
    PrintWriter pw = new PrintWriter(sw);
    t.printStackTrace(pw);
    return sw.toString();
  }
}
