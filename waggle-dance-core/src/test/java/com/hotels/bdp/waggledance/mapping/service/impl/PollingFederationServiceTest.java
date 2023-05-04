/**
 * Copyright (C) 2016-2023 Expedia, Inc.
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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;
import com.hotels.bdp.waggledance.core.federation.service.PopulateStatusFederationService;

@RunWith(MockitoJUnitRunner.class)
public class PollingFederationServiceTest {

  private @Mock PopulateStatusFederationService populateStatusFederationService;

  private PollingFederationService service;

  @Before
  public void setUp() {
    MeterRegistry meterRegistry = new SimpleMeterRegistry();
    service = new PollingFederationService(populateStatusFederationService, meterRegistry);
  }

  @Test
  public void pollNotifyOnStateChange() throws Exception {
    AbstractMetaStore primary = AbstractMetaStore.newPrimaryInstance("p", "uri");
    AbstractMetaStore federate = AbstractMetaStore.newFederatedInstance("f", "uri");
    primary.setStatus(MetaStoreStatus.AVAILABLE);
    federate.setStatus(MetaStoreStatus.AVAILABLE);

    List<AbstractMetaStore> metastores = Lists.newArrayList(primary, federate);
    when(populateStatusFederationService.getAll()).thenReturn(metastores);

    // first time, get base values
    service.poll();
    verify(populateStatusFederationService, never()).update(primary, primary);
    verify(populateStatusFederationService, never()).update(federate, federate);

    // poll with no status changed
    service.poll();
    verify(populateStatusFederationService, never()).update(primary, primary);
    verify(populateStatusFederationService, never()).update(federate, federate);

    // federated flipped status
    federate.setStatus(MetaStoreStatus.UNAVAILABLE);
    service.poll();
    verify(populateStatusFederationService).update(federate, federate);

    // primary flipped status
    primary.setStatus(MetaStoreStatus.UNAVAILABLE);
    service.poll();
    verify(populateStatusFederationService).update(primary, primary);
  }

  @Test
  public void pollNotifyOnStateChangeStatusChangedTwice() throws Exception {
    AbstractMetaStore primary = AbstractMetaStore.newPrimaryInstance("p", "uri");
    AbstractMetaStore federate = AbstractMetaStore.newFederatedInstance("f", "uri");
    primary.setStatus(MetaStoreStatus.AVAILABLE);
    federate.setStatus(MetaStoreStatus.AVAILABLE);

    List<AbstractMetaStore> metastores = Lists.newArrayList(primary, federate);
    when(populateStatusFederationService.getAll()).thenReturn(metastores);

    // first time, get base values
    service.poll();

    // federated flipped status
    federate.setStatus(MetaStoreStatus.UNAVAILABLE);
    service.poll();

    // federated flipped status
    federate.setStatus(MetaStoreStatus.AVAILABLE);
    service.poll();
    verify(populateStatusFederationService, times(2)).update(federate, federate);
  }

}
