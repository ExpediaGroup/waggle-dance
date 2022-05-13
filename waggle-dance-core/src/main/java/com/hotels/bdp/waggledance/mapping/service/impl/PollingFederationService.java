/**
 * Copyright (C) 2016-2022 Expedia, Inc.
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

import static com.hotels.bdp.waggledance.api.model.MetaStoreStatus.AVAILABLE;
import static com.hotels.bdp.waggledance.api.model.MetaStoreStatus.UNAVAILABLE;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.ImmutableTag;
import io.micrometer.core.instrument.MeterRegistry;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;
import com.hotels.bdp.waggledance.core.federation.service.PopulateStatusFederationService;

@Service
public class PollingFederationService {

  private final static Logger log = LoggerFactory.getLogger(PollingFederationService.class);
  private final static String METASTORE_STATUS_METRIC_NAME = "metastore_status";
  private final static String METASTORE_TAG_NAME = "metastore";

  private final PopulateStatusFederationService populateStatusFederationService;
  private Map<String, MetaStoreStatus> previous = new HashMap<>();
  private @Autowired MeterRegistry meterRegistry;

  public PollingFederationService(PopulateStatusFederationService populateStatusFederationService) {
    this.populateStatusFederationService = populateStatusFederationService;
  }

  public void poll() {
    log.debug("polling status");
    Map<String, MetaStoreStatus> current = new HashMap<>();
    List<AbstractMetaStore> metastores = populateStatusFederationService.getAll();
    for (AbstractMetaStore metaStore : metastores) {
      String metastoreName = metaStore.getName();
      MetaStoreStatus metastoreStatus = metaStore.getStatus();
      current.put(metastoreName, metastoreStatus);
      sendMetric(metastoreName, metastoreStatus);
      MetaStoreStatus previousMetastoreStatus = previous.get(metaStore.getName());
      if (previousMetastoreStatus != null) {
        if (previousMetastoreStatus != metaStore.getStatus()) {
          populateStatusFederationService.update(metaStore, metaStore);
        }
      }
    }
    previous = current;
  }

  private void sendMetric(String metastoreName, MetaStoreStatus status) {
    ImmutableTag tag = new ImmutableTag(METASTORE_TAG_NAME, metastoreName);
    Counter counter = Counter.builder(METASTORE_STATUS_METRIC_NAME)
        .tag(tag.getKey(), tag.getValue())
        .register(meterRegistry);
    counter.increment(statusToInt(status));
  }

  private int statusToInt(MetaStoreStatus status) {
    if (status == AVAILABLE) {
      return 0;
    } else if (status == UNAVAILABLE) {
      return 1;
    }
    return 3;
  }

  @VisibleForTesting
  void setMeterRegistry(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }
}
