/**
 * Copyright (C) 2016-2018 Expedia Inc.
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

import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import com.hotels.bdp.waggledance.api.model.DatabaseResolution;
import com.hotels.bdp.waggledance.server.MetaStoreProxyServer;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "")
public class WaggleDanceConfiguration {

  private @NotNull @Min(1) Integer port = MetaStoreProxyServer.DEFAULT_WAGGLEDANCE_PORT;
  private boolean verbose;
  private @Min(1) int disconnectConnectionDelay = 5;
  private @NotNull TimeUnit disconnectTimeUnit = TimeUnit.MINUTES;
  private Map<String, String> configurationProperties;
  private @NotNull DatabaseResolution databaseResolution = DatabaseResolution.MANUAL;

  // Defaults taken from org.apache.thrift.server.TThreadPoolServer
  // Cannot set the unit but it is in seconds.
  private int thriftServerStopTimeoutValInSeconds = 60;
  private int thriftServerRequestTimeout = 20;
  private TimeUnit thriftServerRequestTimeoutUnit = TimeUnit.SECONDS;

  public Integer getPort() {
    return port;
  }

  public void setPort(Integer port) {
    this.port = port;
  }

  public boolean isVerbose() {
    return verbose;
  }

  public void setVerbose(boolean verbose) {
    this.verbose = verbose;
  }

  public int getDisconnectConnectionDelay() {
    return disconnectConnectionDelay;
  }

  public void setDisconnectConnectionDelay(int disconnectConnectionDelay) {
    this.disconnectConnectionDelay = disconnectConnectionDelay;
  }

  public TimeUnit getDisconnectTimeUnit() {
    return disconnectTimeUnit;
  }

  public void setDisconnectTimeUnit(TimeUnit disconnectTimeUnit) {
    this.disconnectTimeUnit = disconnectTimeUnit;
  }

  public Map<String, String> getConfigurationProperties() {
    return configurationProperties;
  }

  public void setConfigurationProperties(Map<String, String> configurationProperties) {
    this.configurationProperties = configurationProperties;
  }

  public void setDatabaseResolution(DatabaseResolution databaseResolution) {
    this.databaseResolution = databaseResolution;
  }

  public DatabaseResolution getDatabaseResolution() {
    return databaseResolution;
  }

  public int getThriftServerStopTimeoutValInSeconds() {
    return thriftServerStopTimeoutValInSeconds;
  }

  public void setThriftServerStopTimeoutValInSeconds(int thriftServerStopTimeoutValInSeconds) {
    this.thriftServerStopTimeoutValInSeconds = thriftServerStopTimeoutValInSeconds;
  }

  public int getThriftServerRequestTimeout() {
    return thriftServerRequestTimeout;
  }

  public void setThriftServerRequestTimeout(int thriftServerRequestTimeout) {
    this.thriftServerRequestTimeout = thriftServerRequestTimeout;
  }

  public TimeUnit getThriftServerRequestTimeoutUnit() {
    return thriftServerRequestTimeoutUnit;
  }

  public void setThriftServerRequestTimeoutUnit(TimeUnit thriftServerRequestTimeoutUnit) {
    this.thriftServerRequestTimeoutUnit = thriftServerRequestTimeoutUnit;
  }

}
