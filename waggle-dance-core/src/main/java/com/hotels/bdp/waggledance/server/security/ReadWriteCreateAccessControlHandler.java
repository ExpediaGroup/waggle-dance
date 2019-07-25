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
package com.hotels.bdp.waggledance.server.security;

import java.util.ArrayList;
import java.util.List;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;

public class ReadWriteCreateAccessControlHandler implements AccessControlHandler {

  private final FederationService federationService;
  private AbstractMetaStore metaStore;

  ReadWriteCreateAccessControlHandler(AbstractMetaStore metaStore,
      FederationService federationService) {
    this.metaStore = metaStore;
    this.federationService = federationService;
  }

  @Override
  public boolean hasWritePermission(String databaseName) {
    return true;
  }

  @Override
  public boolean hasCreatePermission() {
    return true;
  }

  @Override
  public void databaseCreatedNotification(String name) {
    // notify to update mapped databases
    System.out.println("ReadWriteCreateAccessControlHandler.databaseCreatedNotification was called");
    List<String> mappedDatabases = null;
    if (metaStore.getMappedDatabases() != null) {
      mappedDatabases = new ArrayList<>(metaStore.getMappedDatabases());
      if (!mappedDatabases.contains(name)) {
        mappedDatabases.add(name);
      }
    }

    AbstractMetaStore newMetaStore;
    if (metaStore instanceof PrimaryMetaStore) {
      newMetaStore = new PrimaryMetaStore(metaStore.getName(), metaStore.getRemoteMetaStoreUris(),
          metaStore.getAccessControlType());
      newMetaStore.setMappedDatabases(mappedDatabases);
    } else {
      throw new WaggleDanceException(
          String.format("metastore type %s does not support Database creation", metaStore.getClass().getName()));
    }

    federationService.update(metaStore, newMetaStore);
    metaStore = newMetaStore;
  }

}
