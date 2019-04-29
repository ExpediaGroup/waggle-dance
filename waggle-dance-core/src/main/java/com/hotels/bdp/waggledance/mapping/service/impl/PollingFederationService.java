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
package com.hotels.bdp.waggledance.mapping.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;
import com.hotels.bdp.waggledance.core.federation.service.PopulateStatusFederationService;

public class PollingFederationService {

  private final static Logger log = LoggerFactory.getLogger(PollingFederationService.class);

  private final PopulateStatusFederationService populateStatusFederationService;
  private Map<String, MetaStoreStatus> previous = new HashMap<>();

  public PollingFederationService(PopulateStatusFederationService populateStatusFederationService) {
    this.populateStatusFederationService = populateStatusFederationService;
  }

  public void poll() {
    log.debug("polling status");
    Map<String, MetaStoreStatus> current = new HashMap<>();
    List<AbstractMetaStore> metastores = populateStatusFederationService.getAll();
    for (AbstractMetaStore metaStore : metastores) {
      current.put(metaStore.getName(), metaStore.getStatus());
      MetaStoreStatus previousMetastoreStatus = previous.get(metaStore.getName());
      if (previousMetastoreStatus != null) {
        if (previousMetastoreStatus != metaStore.getStatus()) {
          populateStatusFederationService.update(metaStore, metaStore);
        }
      }
    }
    previous = current;
  }
}
