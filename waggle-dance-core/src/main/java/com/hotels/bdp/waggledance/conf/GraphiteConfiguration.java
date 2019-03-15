/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.waggledance.conf;

import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "graphite")
public class GraphiteConfiguration {

  private boolean enabled = false;
  private @Min(1) int port = 2003;
  private String host;
  private String prefix;
  private @Min(1) long pollInterval = 5000;
  private @NotNull TimeUnit pollIntervalTimeUnit = TimeUnit.MILLISECONDS;

  @PostConstruct
  public void init() {
    if ((host != null) || (prefix != null)) {
      enabled = true;
    }
  }

  public boolean isEnabled() {
    return enabled;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getPrefix() {
    return prefix;
  }

  public void setPrefix(String prefix) {
    this.prefix = prefix;
  }

  public long getPollInterval() {
    return pollInterval;
  }

  public void setPollInterval(long pollInterval) {
    this.pollInterval = pollInterval;
  }

  public TimeUnit getPollIntervalTimeUnit() {
    return pollIntervalTimeUnit;
  }

  public void setPollIntervalTimeUnit(TimeUnit pollIntervalTimeUnit) {
    this.pollIntervalTimeUnit = pollIntervalTimeUnit;
  }

}
