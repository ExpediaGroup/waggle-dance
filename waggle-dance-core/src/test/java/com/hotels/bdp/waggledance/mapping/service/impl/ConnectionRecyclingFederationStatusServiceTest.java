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
package com.hotels.bdp.waggledance.mapping.service.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.client.pool.MetaStoreClientPool;

@RunWith(MockitoJUnitRunner.class)
public class ConnectionRecyclingFederationStatusServiceTest {

  private @Mock MetaStoreClientPool metaStoreClientPool;
  private @Mock CloseableThriftHiveMetastoreIface client;

  private ConnectionRecyclingFederationStatusService service;
  private final FederatedMetaStore metaStore = newFederatedInstance("remote", "uri");

  @Before
  public void setUp() {
    service = new ConnectionRecyclingFederationStatusService(metaStoreClientPool);
    when(metaStoreClientPool.borrowObjectUnchecked(metaStore)).thenReturn(client);
  }

  @Test
  public void checkStatusAvailable() throws Exception {
    when(client.isOpen()).thenReturn(true);
    MetaStoreStatus status = service.checkStatus(metaStore);
    assertThat(status, is(MetaStoreStatus.AVAILABLE));
    verify(metaStoreClientPool).returnObjectUnchecked(metaStore, client);
  }

  @Test
  public void checkStatusUnavailable() throws Exception {
    when(client.isOpen()).thenReturn(false);
    MetaStoreStatus status = service.checkStatus(metaStore);
    assertThat(status, is(MetaStoreStatus.UNAVAILABLE));
    verify(metaStoreClientPool).returnObjectUnchecked(metaStore, client);
  }

  @Test
  public void checkStatusUnavailableViaException() throws Exception {
    when(client.isOpen()).thenThrow(new RuntimeException("oh no metastore down!"));
    MetaStoreStatus status = service.checkStatus(metaStore);
    assertThat(status, is(MetaStoreStatus.UNAVAILABLE));
    verify(metaStoreClientPool).returnObjectUnchecked(metaStore, client);
  }

}
