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
package com.hotels.bdp.waggledance.client.tunnelling;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.client.MetaStoreClientFactory;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;
import com.hotels.hcommon.ssh.SshSettings;
import com.hotels.hcommon.ssh.TunnelableSupplier;

@RunWith(MockitoJUnitRunner.class)
public class TunnelingMetaStoreClientFactoryTest {

  public @Rule ThriftHiveMetaStoreJUnitRule metastore = new ThriftHiveMetaStoreJUnitRule();

  private @Mock MetaStoreClientFactory metaStoreClientFactory;
  private @Mock HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory;
  private @Mock HiveMetaStoreClientSupplier hiveMetaStoreClientSupplier;
  private @Mock HiveConf localHiveConf;
  private HiveConf hiveConf;
  private TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory;
  private SshSettings sshSettings;

  private final String metastoreUri = "thrift://metastore-host:42";
  private final String name = "test";
  private final int reconnectionRetries = 10;
  private final String localhost = "my-machine";

  @Before
  public void init() {
    hiveConf = metastore.conf();
    when(localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS)).thenReturn(metastoreUri);
    when(hiveMetaStoreClientSupplierFactory.newInstance(any(), eq(name), eq(reconnectionRetries)))
        .thenReturn(hiveMetaStoreClientSupplier);

    sshSettings = SshSettings
        .builder()
        .withPrivateKeys("private_key")
        .withKnownHosts("")
        .withRoute("user@hop1 -> hop2")
        .withLocalhost(localhost)
        .build();

    tunnelingMetaStoreClientFactory = new TunnelingMetaStoreClientFactory(metaStoreClientFactory,
        hiveMetaStoreClientSupplierFactory);
  }

  @Test
  public void newInstanceWithTunneling() throws Exception {
    ArgumentCaptor<TunnelableSupplier> captor = ArgumentCaptor.forClass(TunnelableSupplier.class);
    CloseableThriftHiveMetastoreIface closeableThriftHiveMetastoreIface = tunnelingMetaStoreClientFactory
        .newInstanceWithTunnelling(hiveConf, name, reconnectionRetries, sshSettings);

    System.out.print(closeableThriftHiveMetastoreIface);
    // verify(tunnelableFactory)
    // .wrap(captor.capture(), eq(tunnelingMetaStoreClientFactory.METHOD_CHECKER), eq("my-machine"), anyInt(),
    // eq("localhost"), eq(metastore.getThriftPort()));
  }
  //
  // @Test
  // public void localHiveConfigUsesCorrectParameters() {
  // tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, reconnectionRetries);
  // ArgumentCaptor<String> stringArgument = ArgumentCaptor.forClass(String.class);
  // verify(localHiveConfFactory).newInstance(stringArgument.capture(), anyInt(), eq(hiveConf));
  // assertThat(stringArgument.getValue(), is(localhost));
  // }
  //
  // @Test
  // public void tunnelableFactorySupplierUsesCorrectParameters() {
  // tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, reconnectionRetries);
  // verify(tunnelableFactorySupplier).get(eq(localHiveConf));
  // }
  //
  // @Test
  // public void hiveMetaStoreClientSupplierFactoryUsesCorrectParameters() {
  // tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, reconnectionRetries);
  // verify(hiveMetaStoreClientSupplierFactory).newInstance(eq(localHiveConf), eq(name), eq(reconnectionRetries));
  // }
  //
  // @Test
  // public void tunnelableFactoryUsesCorrectParameters() throws URISyntaxException {
  // URI uri = new URI(metastore.getThriftConnectionUri());
  // String remoteHost = uri.getHost();
  // int remotePort = uri.getPort();
  // tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, reconnectionRetries);
  // verify(tunnelableFactory)
  // .wrap(eq(hiveMetaStoreClientSupplier), eq(tunnelingMetaStoreClientFactory.METHOD_CHECKER), eq(localhost),
  // any(Integer.class), eq(remoteHost), eq(remotePort));
  // }
  //
  // @Test
  // public void newInstanceWithoutTunneling() throws Exception {
  // hiveConf.unset(WaggleDanceHiveConfVars.SSH_ROUTE.varname);
  // hiveConf.unset(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname);
  // tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, reconnectionRetries);
  // verify(metaStoreClientFactory).newInstance(hiveConf, name, reconnectionRetries);
  // verifyZeroInteractions(tunnelableFactorySupplier);
  // }
  //
  // @Test
  // public void newInstanceWithoutTunnelingNorSupplier() throws ConfigValSecurityException, TException {
  // hiveConf.unset(WaggleDanceHiveConfVars.SSH_ROUTE.varname);
  // hiveConf.unset(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname);
  //
  // TunnelingMetaStoreClientFactory clientFactory = new TunnelingMetaStoreClientFactory();
  // CloseableThriftHiveMetastoreIface closeableThriftHiveMetastoreIface = clientFactory
  // .newInstance(hiveConf, name, reconnectionRetries);
  // assertNotNull(closeableThriftHiveMetastoreIface);
  // }

  @Test(expected = IllegalStateException.class)
  public void newInstanceWithoutTunnelling() {
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, reconnectionRetries);
  }

}
