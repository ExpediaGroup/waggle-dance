package com.hotels.bdp.waggledance.extensions.client.ratelimit;

public enum Metrics {
  
  CALLS("calls"),
  EXCEEDED("exceeded"),
  ERRORS("errors");

  private final static String METRIC_BASE_NAME = "com.hotels.bdp.waggledance.extensions.client.ratelimit";
  private String metricName;
  
  private Metrics(String name) {
    this.metricName = METRIC_BASE_NAME + "." + name;
  }
  
  public String getMetricName() {
    return metricName;
  }
  
}
