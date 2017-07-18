/**
 * Copyright (C) 2016-2017 Expedia Inc.
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
package com.hotels.bdp.waggledance.server.security;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;

public class DatabaseWhitelistAccessControlHandler implements AccessControlHandler {

  private final Set<String> databaseWhiteList = new HashSet<>();
  private final FederationService federationService;
  private PrimaryMetaStore primaryMetaStore;
  private final boolean hasCreatePermission;

  public DatabaseWhitelistAccessControlHandler(PrimaryMetaStore primaryMetaStore, FederationService federationService,
      boolean hasCreatePermission) {
    this.primaryMetaStore = primaryMetaStore;
    this.federationService = federationService;
    this.hasCreatePermission = hasCreatePermission;
    for (String databaseName : primaryMetaStore.getWritableDatabaseWhiteList()) {
      add(databaseName);
    }
  }

  private void add(String databaseName) {
    databaseWhiteList.add(databaseName.trim().toLowerCase());
  }

  @Override
  public boolean hasWritePermission(String databaseName) {
    if (databaseName == null) {
      return true;
    }
    return databaseWhiteList.contains(databaseName.trim().toLowerCase());
  }

  @Override
  public boolean hasCreatePermission() {
    return hasCreatePermission;
  }

  @Override
  public void databaseCreatedNotification(String name) {
    List<String> whiteList = new ArrayList<>(primaryMetaStore.getWritableDatabaseWhiteList());
    String nameLowerCase = name.trim().toLowerCase();
    if (!whiteList.contains(nameLowerCase)) {
      whiteList.add(nameLowerCase);
    }
    PrimaryMetaStore newPrimaryMetastore = new PrimaryMetaStore(primaryMetaStore);
    newPrimaryMetastore.setWritableDatabaseWhiteList(whiteList);
    federationService.update(primaryMetaStore, newPrimaryMetastore);
    primaryMetaStore = newPrimaryMetastore;
    add(nameLowerCase);
  }

}
