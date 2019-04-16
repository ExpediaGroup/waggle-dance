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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.federation.service.FederationStatusService;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;

@RunWith(MockitoJUnitRunner.class)
public class PopulateStatusFederationServiceTest {

  private @Mock FederationService federationService;
  private @Mock FederationStatusService federationStatusService;
  private AbstractMetaStore federatedMetaStore1;
  private AbstractMetaStore federatedMetaStore2;

  private PopulateStatusFederationService service;

  @Before
  public void init() {
    federatedMetaStore1 = AbstractMetaStore.newFederatedInstance("name1", "uri");
    federatedMetaStore2 = AbstractMetaStore.newFederatedInstance("name2", "uri");
    when(federationService.get("federatedMetaStore1")).thenReturn(federatedMetaStore1);
    when(federationService.getAll()).thenReturn(Arrays.asList(federatedMetaStore1, federatedMetaStore2));
    when(federationStatusService.checkStatus(federatedMetaStore1)).thenReturn(MetaStoreStatus.AVAILABLE);
    when(federationStatusService.checkStatus(federatedMetaStore2)).thenReturn(MetaStoreStatus.UNAVAILABLE);
    service = new PopulateStatusFederationService(federationService, federationStatusService);
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
    assertThat(federatedMetaStore1.getStatus(), is(MetaStoreStatus.AVAILABLE));
  }

  @Test
  public void getAll() {
    service.getAll();
    verify(federationService).getAll();
    verify(federationStatusService).checkStatus(federatedMetaStore1);
    assertThat(federatedMetaStore1.getStatus(), is(MetaStoreStatus.AVAILABLE));
    verify(federationStatusService).checkStatus(federatedMetaStore2);
    assertThat(federatedMetaStore2.getStatus(), is(MetaStoreStatus.UNAVAILABLE));
  }

}
