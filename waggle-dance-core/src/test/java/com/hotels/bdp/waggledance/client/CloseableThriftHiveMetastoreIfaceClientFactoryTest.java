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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;

import java.util.Collections;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;
import com.hotels.hcommon.hive.metastore.client.tunnelling.TunnellingMetaStoreClientSupplier;
import com.hotels.hcommon.ssh.SshSettings;

@RunWith(MockitoJUnitRunner.class)
public class CloseableThriftHiveMetastoreIfaceClientFactoryTest {

  private static final String THRIFT_URI = "thrift://1234";

  private @Captor ArgumentCaptor<HiveConf> hiveConfCaptor;
  private @Mock DefaultMetaStoreClientFactory metaStoreClientFactory;
  private final String localhost = "local-machine";
  private final String route = "a -> b -> c";
  private final String knownHosts = "knownHosts";
  private final String privateKeys = "privateKey";
  private final int timeout = 123;
  private final int port = 2222;
  private final MetastoreTunnel metastoreTunnel = createMetastoreTunnel();
  private CloseableThriftHiveMetastoreIfaceClientFactory factory;

  @Before
  public void setUp() {
    factory = new CloseableThriftHiveMetastoreIfaceClientFactory();
  }

  @Test
  public void hiveConf() throws Exception {
    factory.newInstance(newFederatedInstance("fed1", THRIFT_URI));
    MetaStoreClientFactory defaultMetaStoreFactory = factory.getMetaStoreClientFactory();
    assertThat(defaultMetaStoreFactory, instanceOf(DefaultMetaStoreClientFactory.class));

    HiveConf hiveConf = factory.getHiveConf();
    assertThat(hiveConf.getVar(ConfVars.METASTOREURIS), is(THRIFT_URI));
    assertNull(factory.getSshSettings());
  }

  // TODO: make it so the test doesn't actually try to create a tunnel; the tunnel would fail

  @Test
  public void hiveConfForTunneling() throws Exception {
    FederatedMetaStore federatedMetaStore = newFederatedInstance("fed1", THRIFT_URI);
    federatedMetaStore.setMetastoreTunnel(metastoreTunnel);
    factory.newInstance(federatedMetaStore);

    MetaStoreClientFactory tunnelledMetaStoreFactory = factory.getMetaStoreClientFactory();
    assertThat(tunnelledMetaStoreFactory, instanceOf(TunnellingMetaStoreClientSupplier.class));

    HiveConf hiveConf = factory.getHiveConf();
    SshSettings sshSettings = factory.getSshSettings();
    assertThat(hiveConf.getVar(ConfVars.METASTOREURIS), is(THRIFT_URI));
    checkSshSettingsParameters(sshSettings);
    assertThat(sshSettings.isStrictHostKeyChecking(), is(true));
  }

  @Test
  public void hiveConfWithTunnellingAndNoStrictHostKeyChecking() {
    metastoreTunnel.setStrictHostKeyChecking("no");
    AbstractMetaStore federatedMetaStore = newFederatedInstance("fed1", THRIFT_URI);
    federatedMetaStore.setMetastoreTunnel(metastoreTunnel);
    factory.newInstance(federatedMetaStore);

    SshSettings sshSettings = factory.getSshSettings();
    checkSshSettingsParameters(sshSettings);
    assertThat(sshSettings.isStrictHostKeyChecking(), is(false));
  }

  private void checkSshSettingsParameters(SshSettings sshSettings) {
    assertThat(sshSettings.getLocalhost(), is(localhost));
    assertThat(sshSettings.getSshPort(), is(port));
    assertThat(sshSettings.getRoute(), is(route));
    assertThat(sshSettings.getKnownHosts(), is(knownHosts));
    assertThat(sshSettings.getPrivateKeys(), is(Collections.singletonList(privateKeys)));
    assertThat(sshSettings.getSessionTimeout(), is(timeout));
  }

  private MetastoreTunnel createMetastoreTunnel() {
    MetastoreTunnel metastoreTunnel = new MetastoreTunnel();
    metastoreTunnel.setLocalhost(localhost);
    metastoreTunnel.setPort(port);
    metastoreTunnel.setRoute(route);
    metastoreTunnel.setKnownHosts(knownHosts);
    metastoreTunnel.setPrivateKeys(privateKeys);
    metastoreTunnel.setTimeout(timeout);
    return metastoreTunnel;
  }

}
