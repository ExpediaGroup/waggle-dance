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
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIfaceClientFactory;
import com.hotels.hcommon.hive.metastore.client.api.CloseableThriftHiveMetastoreIface;

@RunWith(MockitoJUnitRunner.class)
public class SimpleFederationStatusServiceTest {

  private @Mock
  CloseableThriftHiveMetastoreIfaceClientFactory metaStoreClientFactory;
  private @Mock
  CloseableThriftHiveMetastoreIface client;
  private SimpleFederationStatusService service;

  private final FederatedMetaStore metaStore = FederatedMetaStore.newFederatedInstance("remote", "uri");

  @Before
  public void setUp() {
    service = new SimpleFederationStatusService(metaStoreClientFactory);
    when(metaStoreClientFactory.newInstance(metaStore)).thenReturn(client);
  }

  @Test
  public void checkStatusAvailable() throws Exception {
    when(client.isOpen()).thenReturn(true);
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
