/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.spy;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;
import com.hotels.bdp.waggledance.metastore.ThriftHiveMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

@RunWith(MockitoJUnitRunner.class)
public class SimpleFederationStatusServiceTest {

  private @Mock CloseableMetaStoreClient client;
  private @Mock ThriftHiveMetaStoreClientFactory factory;
  private SimpleFederationStatusService service;

  private final FederatedMetaStore metaStore = FederatedMetaStore.newFederatedInstance("remote",
      "thrift://localhost:9083");

  @Before
  public void setUp() {
    service = spy(new SimpleFederationStatusService(factory));
  }

  @Test
  public void checkStatusAvailable() {
    when(client.isOpen()).thenReturn(true);
    when(factory.newInstance(eq(metaStore))).thenReturn(client);
    MetaStoreStatus status = service.checkStatus(metaStore);
    assertThat(status, is(MetaStoreStatus.AVAILABLE));
  }

  @Test
  public void checkStatusUnavailable() throws Exception {
    when(client.isOpen()).thenReturn(false);
    MetaStoreStatus status = service.checkStatus(metaStore);
    assertThat(status, is(MetaStoreStatus.UNAVAILABLE));
  }

  @Test
  public void checkStatusUnavailableViaException() throws Exception {
    when(client.isOpen()).thenThrow(new RuntimeException("oh no metastore down!"));
    MetaStoreStatus status = service.checkStatus(metaStore);
    assertThat(status, is(MetaStoreStatus.UNAVAILABLE));
  }

}
