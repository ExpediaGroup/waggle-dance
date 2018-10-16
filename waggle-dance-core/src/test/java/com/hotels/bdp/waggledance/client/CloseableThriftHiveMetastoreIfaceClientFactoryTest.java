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
package com.hotels.bdp.waggledance.client;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;

import java.lang.reflect.Proxy;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.client.tunnelling.TunnelingMetaStoreClientFactory;

@RunWith(MockitoJUnitRunner.class)
public class CloseableThriftHiveMetastoreIfaceClientFactoryTest {

  private static final String THRIFT_URI = "thrift://localhost:1234";
  public @Rule ExpectedException expectedException = ExpectedException.none();

  public @Captor ArgumentCaptor<HiveConf> hiveConfCaptor;
  private @Mock MetastoreClientFactoryHelper helper;
  private @Mock TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory;
  private @Mock FederatedMetaStore federatedMetaStore;
  private @Mock CloseableThriftHiveMetastoreIface closeableThriftHiveMetastoreIface;
  private CloseableThriftHiveMetastoreIfaceClientFactory factory;

  @Before
  public void setUp() {
    factory = new CloseableThriftHiveMetastoreIfaceClientFactory();
  }

  @Test
  public void hiveConf() throws Exception {
    AbstractMetaStore metaStore = newFederatedInstance("fed1", THRIFT_URI);
    MetastoreClientFactoryHelper helper = new MetastoreClientFactoryHelper(metaStore);
    CloseableThriftHiveMetastoreIface result = factory.newInstance(metaStore, helper);
    assertThat(result, instanceOf(Proxy.class));
  }

  @Test
  public void hiveConfForTunneling() throws Exception {
    when(federatedMetaStore.getName()).thenReturn("fed1");
    when(federatedMetaStore.getRemoteMetaStoreUris()).thenReturn(THRIFT_URI);
    when(helper.get()).thenReturn(tunnelingMetaStoreClientFactory);
    when(tunnelingMetaStoreClientFactory.newInstance(hiveConfCaptor.capture(), eq("waggledance-fed1"), eq(3)))
        .thenReturn(closeableThriftHiveMetastoreIface);
    CloseableThriftHiveMetastoreIface result = factory.newInstance(federatedMetaStore, helper);
    assertThat(hiveConfCaptor.getValue().get(ConfVars.METASTOREURIS.varname), is(THRIFT_URI));
    assertThat(result, is(closeableThriftHiveMetastoreIface));
  }

}
