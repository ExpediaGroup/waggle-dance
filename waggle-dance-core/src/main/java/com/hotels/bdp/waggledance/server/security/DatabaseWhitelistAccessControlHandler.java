/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
import java.util.List;
import java.util.Locale;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;
import com.hotels.bdp.waggledance.util.AllowList;

public class DatabaseWhitelistAccessControlHandler implements AccessControlHandler {

  private final FederationService federationService;
  private final boolean hasCreatePermission;
  private final AllowList writableDatabaseWhiteList;
  private AbstractMetaStore metaStore;

  DatabaseWhitelistAccessControlHandler(
      AbstractMetaStore metaStore,
      FederationService federationService,
      boolean hasCreatePermission) {
    this.metaStore = metaStore;
    this.federationService = federationService;
    this.hasCreatePermission = hasCreatePermission;
    writableDatabaseWhiteList = new AllowList(metaStore.getWritableDatabaseWhiteList());
  }

  private String trimToLowerCase(String string) {
    return string.trim().toLowerCase(Locale.ROOT);
  }

  @Override
  public boolean hasWritePermission(String databaseName) {
    return writableDatabaseWhiteList.contains(databaseName);
  }

  @Override
  public boolean hasCreatePermission() {
    return hasCreatePermission;
  }

  @Override
  public void databaseCreatedNotification(String name) {
    List<String> newWritableDatabaseWhiteList = new ArrayList<>(metaStore.getWritableDatabaseWhiteList());
    List<String> mappedDatabases = null;
    String nameLowerCase = trimToLowerCase(name);
    if (!newWritableDatabaseWhiteList.contains(nameLowerCase)) {
      newWritableDatabaseWhiteList.add(nameLowerCase);
    }
    if (metaStore.getMappedDatabases() != null) {
      mappedDatabases = new ArrayList<>(metaStore.getMappedDatabases());
      if (!mappedDatabases.contains(name)) {
        mappedDatabases.add(name);
      }
    }

    AbstractMetaStore newMetaStore;
    if (metaStore instanceof PrimaryMetaStore) {
      newMetaStore = new PrimaryMetaStore(metaStore.getName(), metaStore.getRemoteMetaStoreUris(),
          metaStore.getAccessControlType(), newWritableDatabaseWhiteList);
      newMetaStore.setMappedDatabases(mappedDatabases);
    } else {
      throw new WaggleDanceException(
          String.format("Metastore type %s does not support database creation", metaStore.getClass().getName()));
    }

    federationService.update(metaStore, newMetaStore);
    metaStore = newMetaStore;
    writableDatabaseWhiteList.add(nameLowerCase);
  }

}
