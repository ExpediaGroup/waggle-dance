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
package com.hotels.bdp.waggledance.api.model;

import static com.hotels.bdp.waggledance.api.model.ConnectionType.DIRECT;
import static com.hotels.bdp.waggledance.api.model.ConnectionType.TUNNELED;

import java.beans.Transient;
import java.util.Collections;
import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "federationType")
@JsonSubTypes({
    @Type(value = PrimaryMetaStore.class, name = "PRIMARY"),
    @Type(value = FederatedMetaStore.class, name = "FEDERATED") })
public abstract class AbstractMetaStore {

  private String databasePrefix;
  private List<String> writableDatabaseWhitelist;
  private @NotBlank String name;
  private @NotBlank String remoteMetaStoreUris;
  private @Valid MetastoreTunnel metastoreTunnel;
  private @NotNull AccessControlType accessControlType = AccessControlType.READ_ONLY;
  private transient @JsonProperty @NotNull MetaStoreStatus status = MetaStoreStatus.UNKNOWN;

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

  public String getDatabasePrefix() {
    return databasePrefix;
  }

  public void setDatabasePrefix(String databasePrefix) {
    this.databasePrefix = databasePrefix;
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

  @Transient
  public MetaStoreStatus getStatus() {
    return status;
  }

  @Transient
  public void setStatus(MetaStoreStatus status) {
    this.status = status;
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
        .add("metastoreTunnel", metastoreTunnel)
        .add("accessControlType", accessControlType)
        .add("writableDatabaseWhiteList", writableDatabaseWhitelist)
        .add("status", status)
        .toString();
  }
}
