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
package com.hotels.bdp.waggledance.core.federation.service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.federation.service.FederationStatusService;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;

@Service
public class PopulateStatusFederationService implements FederationService {

  private final FederationService federationService;
  private final FederationStatusService federationStatusService;

  public PopulateStatusFederationService(
      @Qualifier("notifyingFederationService") FederationService federationService,
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
    // We don't care about order here we just want all the statuses.
    metaStores.parallelStream().forEach(metaStore -> {
      populate(metaStore);
    });
    return new ArrayList<>(metaStores);
  }

  private AbstractMetaStore populate(AbstractMetaStore metaStore) {
    MetaStoreStatus status = federationStatusService.checkStatus(metaStore);
    metaStore.setStatus(status);
    return metaStore;
  }
}
