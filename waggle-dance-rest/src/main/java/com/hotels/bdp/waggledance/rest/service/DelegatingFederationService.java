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
package com.hotels.bdp.waggledance.rest.service;

import java.util.ArrayList;
import java.util.List;

import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.federation.service.FederationStatusService;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;

public class DelegatingFederationService implements FederationService {

  private final FederationService federationService;
  private final FederationStatusService federationStatusService;

  public DelegatingFederationService(
      FederationService federationService,
      FederationStatusService federationStatusService) {
    this.federationService = federationService;
    this.federationStatusService = federationStatusService;
  }

  @Override
  public void register(AbstractMetaStore federatedMetaStore) {
    federationService.register(federatedMetaStore);
  }

  @Override
  public void update(AbstractMetaStore oldMetaStore, AbstractMetaStore newMetaStore) {
    federationService.update(oldMetaStore, newMetaStore);
  }

  @Override
  public void unregister(String name) {
    federationService.unregister(name);
  }

  @Override
  public AbstractMetaStore get(String name) {
    return populate(federationService.get(name));
  }

  @Override
  public List<AbstractMetaStore> getAll() {
    List<AbstractMetaStore> metaStores = federationService.getAll();
    List<AbstractMetaStore> populatedMetaStores = new ArrayList<>(metaStores.size());
    for (AbstractMetaStore metaStore : metaStores) {
      populatedMetaStores.add(populate(metaStore));
    }
    return populatedMetaStores;
  }

  private AbstractMetaStore populate(AbstractMetaStore metaStore) {
    MetaStoreStatus status = MetaStoreStatus.AVAILABLE; // TODO PD we need to support tunnelled checks
    if (metaStore.getMetastoreTunnel() == null) {
      status = federationStatusService.checkStatus(metaStore.getRemoteMetaStoreUris());
    }
    metaStore.setStatus(status);
    return metaStore;
  }

}
