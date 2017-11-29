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

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.federation.service.FederationStatusService;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;

@RunWith(MockitoJUnitRunner.class)
public class DelegatingFederationServiceTest {

  private static final String FEDERATED_METASTORE1_URI = "thrift://federatedMetaStore1:9083";
  private static final String FEDERATED_METASTORE2_URI = "thrift://federatedMetaStore2:9083";

  private @Mock FederationService federationService;
  private @Mock FederationStatusService federationStatusService;
  private @Mock AbstractMetaStore federatedMetaStore1;
  private @Mock AbstractMetaStore federatedMetaStore2;

  private DelegatingFederationService service;

  @Before
  public void init() {
    when(federatedMetaStore1.getRemoteMetaStoreUris()).thenReturn(FEDERATED_METASTORE1_URI);
    when(federatedMetaStore2.getRemoteMetaStoreUris()).thenReturn(FEDERATED_METASTORE2_URI);
    when(federationService.get("federatedMetaStore1")).thenReturn(federatedMetaStore1);
    when(federationService.get("federatedMetaStore2")).thenReturn(federatedMetaStore2);
    when(federationService.getAll()).thenReturn(Arrays.asList(federatedMetaStore1, federatedMetaStore2));
    when(federationStatusService.checkStatus(federatedMetaStore1)).thenReturn(MetaStoreStatus.AVAILABLE);
    when(federationStatusService.checkStatus(federatedMetaStore2)).thenReturn(MetaStoreStatus.UNAVAILABLE);
    service = new DelegatingFederationService(federationService, federationStatusService);
  }

  @Test
  public void register() {
    service.register(federatedMetaStore1);
    verify(federationService).register(federatedMetaStore1);
    verifyZeroInteractions(federationStatusService);
  }

  @Test
  public void update() {
    service.update(federatedMetaStore1, federatedMetaStore2);
    verify(federationService).update(federatedMetaStore1, federatedMetaStore2);
    verifyZeroInteractions(federationStatusService);
  }

  @Test
  public void unregister() {
    service.unregister("federatedMetaStore1");
    verify(federationService).unregister("federatedMetaStore1");
    verifyZeroInteractions(federationStatusService);
  }

  @Test
  public void get() {
    service.get("federatedMetaStore1");
    verify(federationService).get("federatedMetaStore1");
    verify(federationStatusService).checkStatus(federatedMetaStore1);
    verify(federatedMetaStore1).setStatus(MetaStoreStatus.AVAILABLE);
  }

  @Test
  public void getAll() {
    service.getAll();
    verify(federationService).getAll();
    verify(federationStatusService).checkStatus(federatedMetaStore1);
    verify(federatedMetaStore1).setStatus(MetaStoreStatus.AVAILABLE);
    verify(federationStatusService).checkStatus(federatedMetaStore2);
    verify(federatedMetaStore2).setStatus(MetaStoreStatus.UNAVAILABLE);
  }

}
