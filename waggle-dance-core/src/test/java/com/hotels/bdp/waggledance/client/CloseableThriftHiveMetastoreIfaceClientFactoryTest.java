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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;

import java.util.Collections;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;
import com.hotels.hcommon.ssh.SshSettings;
import com.hotels.hcommon.ssh.TunnelableFactory;

@RunWith(MockitoJUnitRunner.class)
public class CloseableThriftHiveMetastoreIfaceClientFactoryTest {

  private static final String THRIFT_URI = "thrift://host:port";

  private CloseableThriftHiveMetastoreIfaceClientFactory factory;
  private @Mock DefaultMetaStoreClientFactory metaStoreClientFactory;

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
  }

  @Test
  public void hiveConfForTunneling() throws Exception {
    String localhost = "local-machine";
    int port = 2222;
    String route = "a -> b -> c";
    String knownHosts = "knownHosts";
    String privateKeys = "privateKey";
    int timeout = 123;

    MetastoreTunnel metastoreTunnel = new MetastoreTunnel();
    metastoreTunnel.setLocalhost(localhost);
    metastoreTunnel.setPort(port);
    metastoreTunnel.setRoute(route);
    metastoreTunnel.setKnownHosts(knownHosts);
    metastoreTunnel.setPrivateKeys(privateKeys);
    metastoreTunnel.setTimeout(timeout);

    AbstractMetaStore federatedMetaStore = newFederatedInstance("fed1", THRIFT_URI);
    federatedMetaStore.setMetastoreTunnel(metastoreTunnel);
    factory.newInstance(federatedMetaStore);

    ArgumentCaptor<HiveConf> hiveConfCaptor = ArgumentCaptor.forClass(HiveConf.class);
    verify(metaStoreClientFactory).setLocalhost(eq(localhost));
    verify(metaStoreClientFactory).setTunnelableFactory(any(TunnelableFactory.class));
    verify(metaStoreClientFactory).newInstance(hiveConfCaptor.capture(), anyString(), anyInt());

    HiveConf hiveConf = hiveConfCaptor.getValue();
    SshSettings sshSettings = factory.getSshSettings();

    assertThat(hiveConf.getVar(ConfVars.METASTOREURIS), is(THRIFT_URI));
    assertThat(sshSettings.getLocalhost(), is(localhost));
    assertThat(sshSettings.getSshPort(), is(port));
    assertThat(sshSettings.getRoute(), is(route));
    assertThat(sshSettings.getKnownHosts(), is(knownHosts));
    assertThat(sshSettings.getPrivateKeys(), is(Collections.singletonList(privateKeys)));
    assertThat(sshSettings.getSessionTimeout(), is(timeout));
  }

}
