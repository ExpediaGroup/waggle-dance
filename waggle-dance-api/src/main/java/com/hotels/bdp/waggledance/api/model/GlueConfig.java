/**
 * Copyright (C) 2016-2022 Expedia, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
