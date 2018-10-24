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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.client.tunnelling.TunnelingMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;

@RunWith(MockitoJUnitRunner.class)
public class CloseableThriftHiveMetastoreIfaceClientFactoryTest {

  private static final String THRIFT_URI = "thrift://host:port";

  private CloseableThriftHiveMetastoreIfaceClientFactory factory;
  private @Mock TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory;
  private @Mock DefaultMetaStoreClientFactory defaultMetaStoreClientFactory;

  @Before
  public void setUp() {
    factory = new CloseableThriftHiveMetastoreIfaceClientFactory(tunnelingMetaStoreClientFactory,
        defaultMetaStoreClientFactory);
  }

  @Test
  public void defaultFactory() throws Exception {
    ArgumentCaptor<HiveConf> hiveConfCaptor = ArgumentCaptor.forClass(HiveConf.class);

    factory.newInstance(newFederatedInstance("fed1", THRIFT_URI));
    verify(defaultMetaStoreClientFactory).newInstance(hiveConfCaptor.capture(), eq("waggledance-fed1"), eq(3));
    verifyZeroInteractions(tunnelingMetaStoreClientFactory);
    HiveConf hiveConf = hiveConfCaptor.getValue();
    assertThat(hiveConf.getVar(ConfVars.METASTOREURIS), is(THRIFT_URI));
  }

  @Test
  public void tunnelingFactory() throws Exception {
    MetastoreTunnel metastoreTunnel = new MetastoreTunnel();
    metastoreTunnel.setLocalhost("local-machine");
    metastoreTunnel.setPort(2222);
    metastoreTunnel.setRoute("a -> b -> c");
    metastoreTunnel.setKnownHosts("knownHosts");
    metastoreTunnel.setPrivateKeys("privateKeys");
    metastoreTunnel.setTimeout(123);
    AbstractMetaStore federatedMetaStore = newFederatedInstance("fed1", THRIFT_URI);
    federatedMetaStore.setMetastoreTunnel(metastoreTunnel);

    factory.newInstance(federatedMetaStore);
    verify(tunnelingMetaStoreClientFactory).newInstance(THRIFT_URI, metastoreTunnel, "fed1", 3);
    verifyZeroInteractions(defaultMetaStoreClientFactory);
  }

}
