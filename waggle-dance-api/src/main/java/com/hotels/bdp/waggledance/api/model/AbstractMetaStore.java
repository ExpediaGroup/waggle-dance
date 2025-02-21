/**
 * Copyright (C) 2016-2025 Expedia, Inc.
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

import static com.hotels.bdp.waggledance.api.model.ConnectionType.DIRECT;
import static com.hotels.bdp.waggledance.api.model.ConnectionType.TUNNELED;

import java.beans.Transient;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.HashBiMap;

import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "federationType")
@JsonSubTypes({
    @Type(value = PrimaryMetaStore.class, name = "PRIMARY"),
    @Type(value = FederatedMetaStore.class, name = "FEDERATED") })
public abstract class AbstractMetaStore {
  private String databasePrefix;
  private String hiveMetastoreFilterHook;
  private List<String> writableDatabaseWhitelist;
  private List<String> mappedDatabases;
  private @Valid List<MappedTables> mappedTables;
  private Map<String, String> databaseNameMapping = new HashMap<>();
  private @NotBlank String name;
  private @NotBlank String remoteMetaStoreUris;
  private @Valid MetastoreTunnel metastoreTunnel;
  private @NotNull AccessControlType accessControlType = AccessControlType.READ_ONLY;
  private transient @JsonProperty @NotNull MetaStoreStatus status = MetaStoreStatus.UNKNOWN;
  private long latency = 0;
  private transient @JsonIgnore HashBiMap<String, String> databaseNameBiMapping = HashBiMap.create();
  private boolean impersonationEnabled;
  private Map<String, String> configurationProperties = new HashMap<>();
  private String readOnlyRemoteMetaStoreUris;

  public AbstractMetaStore() {}

  public AbstractMetaStore(String name, String remoteMetaStoreUris, AccessControlType accessControlType) {
    this.name = name;
    this.remoteMetaStoreUris = remoteMetaStoreUris;
    this.accessControlType = accessControlType;
  }

  public AbstractMetaStore(
      String name,
      String remoteMetaStoreUris,
      AccessControlType accessControlType,
      List<String> writableDatabaseWhitelist) {
    this.name = name;
    this.remoteMetaStoreUris = remoteMetaStoreUris;
    this.accessControlType = accessControlType;
    this.writableDatabaseWhitelist = writableDatabaseWhitelist;
  }

  public AbstractMetaStore(
          String name,
          String remoteMetaStoreUris,
          String databasePrefix,
          AccessControlType accessControlType,
          List<String> writableDatabaseWhitelist) {
    this.name = name;
    this.remoteMetaStoreUris = remoteMetaStoreUris;
    this.databasePrefix = databasePrefix;
    this.accessControlType = accessControlType;
    this.writableDatabaseWhitelist = writableDatabaseWhitelist;
  }


  public static FederatedMetaStore newFederatedInstance(String name, String remoteMetaStoreUris) {
    return new FederatedMetaStore(name, remoteMetaStoreUris);
  }

  public static PrimaryMetaStore newPrimaryInstance(
      String name,
      String remoteMetaStoreUris,
      AccessControlType accessControlType) {
    return new PrimaryMetaStore(name, remoteMetaStoreUris, accessControlType);
  }

  public static PrimaryMetaStore newPrimaryInstance(String name, String remoteMetaStoreUris) {
    return new PrimaryMetaStore(name, remoteMetaStoreUris, AccessControlType.READ_ONLY);
  }

  public static PrimaryMetaStore newPrimaryInstance(
          String name,
          String remoteMetaStoreUris,
          String databasePrefix,
          AccessControlType accessControlType) {
    return new PrimaryMetaStore(name, remoteMetaStoreUris, databasePrefix, accessControlType);
  }

  public String getDatabasePrefix() {
    return databasePrefix;
  }

  public void setDatabasePrefix(String databasePrefix) {
    this.databasePrefix = databasePrefix;
  }

  public String getHiveMetastoreFilterHook() {
    return hiveMetastoreFilterHook;
  }

  public void setHiveMetastoreFilterHook(String hiveMetastoreFilterHook) {
    this.hiveMetastoreFilterHook = hiveMetastoreFilterHook;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getRemoteMetaStoreUris() {
    return remoteMetaStoreUris;
  }

  public void setRemoteMetaStoreUris(String remoteMetaStoreUris) {
    this.remoteMetaStoreUris = remoteMetaStoreUris;
  }

  public String getReadOnlyRemoteMetaStoreUris() {
    return readOnlyRemoteMetaStoreUris;
  }
  
  public void setReadOnlyRemoteMetaStoreUris(String readOnlyRemoteMetaStoreUris) {
    this.readOnlyRemoteMetaStoreUris = readOnlyRemoteMetaStoreUris;
  }
  
  public MetastoreTunnel getMetastoreTunnel() {
    return metastoreTunnel;
  }

  public void setMetastoreTunnel(MetastoreTunnel metastoreTunnel) {
    this.metastoreTunnel = metastoreTunnel;
  }

  public ConnectionType getConnectionType() {
    if (getMetastoreTunnel() != null) {
      return TUNNELED;
    }
    return DIRECT;
  }

  abstract public FederationType getFederationType();

  public AccessControlType getAccessControlType() {
    return accessControlType;
  }

  public void setAccessControlType(AccessControlType accessControlType) {
    this.accessControlType = accessControlType;
  }

  public List<String> getWritableDatabaseWhiteList() {
    if (writableDatabaseWhitelist == null) {
      return Collections.emptyList();
    }
    return Collections.unmodifiableList(writableDatabaseWhitelist);
  }

  public void setWritableDatabaseWhiteList(List<String> writableDatabaseWhitelist) {
    this.writableDatabaseWhitelist = writableDatabaseWhitelist;
  }

  public long getLatency() {
    return latency;
  }

  public void setLatency(long latency) {
    this.latency = latency;
  }

  public List<String> getMappedDatabases() {
    return mappedDatabases;
  }

  public void setMappedDatabases(List<String> mappedDatabases) {
    this.mappedDatabases = mappedDatabases;
  }

  public List<MappedTables> getMappedTables() {
    return mappedTables;
  }

  public void setMappedTables(List<MappedTables> mappedTables) {
    this.mappedTables = mappedTables;
  }

  public Map<String, String> getDatabaseNameMapping() {
    return databaseNameMapping;
  }

  public void setDatabaseNameMapping(Map<String, String> databaseNameMapping) {
    if (databaseNameMapping == null) {
      databaseNameMapping = Collections.emptyMap();
    }
    this.databaseNameMapping = Collections.unmodifiableMap(databaseNameMapping);
    databaseNameBiMapping = HashBiMap.create(databaseNameMapping);
  }

  @Transient
  public HashBiMap<String, String> getDatabaseNameBiMapping() {
    return databaseNameBiMapping;
  }

  public Map<String, String> getConfigurationProperties() {
    return configurationProperties;
  }

  public void setConfigurationProperties(
          Map<String, String> configurationProperties) {
    this.configurationProperties = configurationProperties;
  }

  @Transient
  public MetaStoreStatus getStatus() {
    return status;
  }

  @Transient
  public void setStatus(MetaStoreStatus status) {
    this.status = status;
  }

  public boolean isImpersonationEnabled() {
    return impersonationEnabled;
  }

  public void setImpersonationEnabled(boolean impersonationEnabled) {
    this.impersonationEnabled = impersonationEnabled;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    final AbstractMetaStore other = (AbstractMetaStore) obj;
    return Objects.equal(name, other.name);
  }

  @Override
  public String toString() {
    return MoreObjects
        .toStringHelper(this)
        .add("name", name)
        .add("databasePrefix", databasePrefix)
        .add("federationType", getFederationType())
        .add("remoteMetaStoreUris", remoteMetaStoreUris)
        .add("readOnlyRemoteMetaStoreUris", readOnlyRemoteMetaStoreUris)
        .add("metastoreTunnel", metastoreTunnel)
        .add("accessControlType", accessControlType)
        .add("writableDatabaseWhiteList", writableDatabaseWhitelist)
        .add("status", status)
        .toString();
  }
}
