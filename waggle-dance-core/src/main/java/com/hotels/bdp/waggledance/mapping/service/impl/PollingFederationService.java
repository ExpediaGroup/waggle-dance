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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.hotels.bdp.waggledance.api.federation.service.FederationStatusService;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.core.federation.service.PopulateStatusFederationService;

@Component
public class PollingFederationService {

  private static final Logger LOG = LoggerFactory.getLogger(PollingFederationService.class);
  private final PopulateStatusFederationService populateStatusFederationService;
  private Map<String, AbstractMetaStore> previous = new HashMap<>();

  @Autowired
  public PollingFederationService(
      NotifyingFederationService notifyingFederationService,
      FederationStatusService federationStatusService) {
    populateStatusFederationService = new PopulateStatusFederationService(notifyingFederationService,
        federationStatusService);
  }

  @Scheduled(fixedDelayString = "${status-polling-delay}")
  public void poll() {
    LOG.info("Started PollingFederationService.poll()");
    Map<String, AbstractMetaStore> current = new HashMap<>();
    List<AbstractMetaStore> metastores = populateStatusFederationService.getAll();
    for (AbstractMetaStore metaStore : metastores) {
      current.put(metaStore.getName(), metaStore);
      AbstractMetaStore previousMetastore = previous.get(metaStore.getName());
      if (previousMetastore != null) {
        LOG.info("previousMetastore is not null");
        LOG.info("previousMetastore \"{}\" status = {}", previousMetastore.getName(), previousMetastore.getStatus());
        LOG.info("metaStore {} status = {}", metaStore.getName(), metaStore.getStatus());
        if (previousMetastore.getStatus() != metaStore.getStatus()) {
          LOG.info("calling update on \"{}\"", metaStore.getName());
          populateStatusFederationService.update(previousMetastore, metaStore);
        }
      }
    }
    previous = current;
  }
}
