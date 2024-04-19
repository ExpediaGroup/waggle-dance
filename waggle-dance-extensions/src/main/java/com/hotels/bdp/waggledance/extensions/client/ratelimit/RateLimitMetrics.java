/**
 * Copyright (C) 2016-2024 Expedia, Inc.
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
package com.hotels.bdp.waggledance.extensions.client.ratelimit;

public enum RateLimitMetrics {

  EXCEEDED("exceeded"),
  ERRORS("errors"),
  WITHIN_LIMIT("within_limit");

  private final static String METRIC_BASE_NAME = "com.hotels.bdp.waggledance.extensions.client.ratelimit";
  private String metricName;

  private RateLimitMetrics(String name) {
    this.metricName = METRIC_BASE_NAME + "." + name;
  }

  public String getMetricName() {
    return metricName;
  }

}
