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
package com.hotels.bdp.waggledance.api.model;

import java.util.Collections;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotBlank;

public class FederatedMetaStore extends AbstractMetaStore {

  private @NotNull List<String> mappedDatabases = Collections.emptyList();

  public FederatedMetaStore() {}

  public FederatedMetaStore(String name, String remoteMetaStoreUris) {
    this(name, remoteMetaStoreUris, AccessControlType.READ_ONLY);
  }

  public FederatedMetaStore(String name, String remoteMetaStoreUris, AccessControlType accessControlType) {
    this(name, remoteMetaStoreUris, accessControlType, Collections.<String>emptyList());
  }

  public FederatedMetaStore(FederatedMetaStore federatedMetaStore) {
    this(federatedMetaStore.getName(), federatedMetaStore.getRemoteMetaStoreUris(),
        federatedMetaStore.getAccessControlType(), federatedMetaStore.getWritableDatabaseWhiteList());
  }

  public FederatedMetaStore(
      String name,
      String remoteMetaStoreUris,
      AccessControlType accessControlType,
      List<String> writeableDatabaseWhiteList) {
    super(name, remoteMetaStoreUris, accessControlType, writeableDatabaseWhiteList);
  }

  @Override
  public FederationType getFederationType() {
    return FederationType.FEDERATED;
  }

  @NotBlank
  @Override
  public String getDatabasePrefix() {
    String prefix = super.getDatabasePrefix();
    if (prefix == null) {
      prefix = getName() + "_";
    }
    return prefix;
  }

  public List<String> getMappedDatabases() {
    return mappedDatabases;
  }

  public void setMappedDatabases(List<String> mappedDatabases) {
    this.mappedDatabases = mappedDatabases;
  }
}
