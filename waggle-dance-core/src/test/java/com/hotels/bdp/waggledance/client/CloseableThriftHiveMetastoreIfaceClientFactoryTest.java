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
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetastoreTunnel;

@RunWith(MockitoJUnitRunner.class)
public class CloseableThriftHiveMetastoreIfaceClientFactoryTest {

  private static final String THRIFT_URI = "thrift://host:port";

  private CloseableThriftHiveMetastoreIfaceClientFactory factory;
  private @Mock
  DefaultMetastoreClientFactory metaStoreClientFactory;

  @Before
  public void setUp() {
    factory = new CloseableThriftHiveMetastoreIfaceClientFactory(metaStoreClientFactory);
  }

  @Test
  public void hiveConf() throws Exception {
    ArgumentCaptor<HiveConf> hiveConfCaptor = ArgumentCaptor.forClass(HiveConf.class);

    factory.newInstance(newFederatedInstance("fed1", THRIFT_URI));
    verify(metaStoreClientFactory).newInstance(hiveConfCaptor.capture(), anyString(), anyInt());

    HiveConf hiveConf = hiveConfCaptor.getValue();
    assertThat(hiveConf.getVar(ConfVars.METASTOREURIS), is(THRIFT_URI));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname), is(nullValue()));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_PORT.varname), is(nullValue()));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname), is(nullValue()));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname), is(nullValue()));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname), is(nullValue()));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_SESSION_TIMEOUT.varname), is(nullValue()));
  }

  @Test
  public void hiveConfForTunneling() throws Exception {
    ArgumentCaptor<HiveConf> hiveConfCaptor = ArgumentCaptor.forClass(HiveConf.class);

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
    verify(metaStoreClientFactory).newInstance(hiveConfCaptor.capture(), anyString(), anyInt());

    HiveConf hiveConf = hiveConfCaptor.getValue();
    assertThat(hiveConf.getVar(ConfVars.METASTOREURIS), is(THRIFT_URI));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname), is("local-machine"));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_PORT.varname), is("2222"));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname), is("a -> b -> c"));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname), is("knownHosts"));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname), is("privateKeys"));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_SESSION_TIMEOUT.varname), is("123"));
  }
}
