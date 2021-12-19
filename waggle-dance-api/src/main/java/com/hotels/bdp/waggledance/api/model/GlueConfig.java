package com.hotels.bdp.waggledance.api.model;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GlueConfig {

  private String glueAccountId;

  // Hive property: aws.glue.endpoint
  private String glueEndpoint;

  public String getGlueAccountId() {
    return glueAccountId;
  }

  public void setGlueAccountId(String glueAccountId) {
    this.glueAccountId = glueAccountId;
  }

  public String getGlueEndpoint() {
    return glueEndpoint;
  }

  public void setGlueEndpoint(String glueEndpoint) {
    this.glueEndpoint = glueEndpoint;
  }

  public Map<String, String> getConfigurationProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put("hive.metastore.glue.catalogid", glueAccountId);
    properties.put("aws.glue.endpoint", glueEndpoint);
    return Collections.unmodifiableMap(properties);
  }

}
