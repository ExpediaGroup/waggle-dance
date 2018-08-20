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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.client.MetaStoreClientFactory;
import com.hotels.bdp.waggledance.client.WaggleDanceHiveConfVars;
import com.hotels.bdp.waggledance.client.tunnelling.HiveMetaStoreClientSupplier;
import com.hotels.bdp.waggledance.client.tunnelling.HiveMetaStoreClientSupplierFactory;
import com.hotels.bdp.waggledance.client.tunnelling.LocalHiveConfFactory;
import com.hotels.bdp.waggledance.client.tunnelling.TunnelableFactorySupplier;
import com.hotels.bdp.waggledance.client.tunnelling.TunnelingMetaStoreClientFactory;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;
import com.hotels.hcommon.ssh.TunnelableFactory;
import com.hotels.hcommon.ssh.TunnelableSupplier;

@RunWith(MockitoJUnitRunner.class)
public class TunnelingMetaStoreClientFactoryTest {

  public @Rule ThriftHiveMetaStoreJUnitRule metastore = new ThriftHiveMetaStoreJUnitRule();

  private @Mock TunnelableFactorySupplier tunnelableFactorySupplier;
  private @Mock MetaStoreClientFactory metaStoreClientFactory;
  private @Mock TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory;
  private @Mock LocalHiveConfFactory localHiveConfFactory;
  private @Mock HiveMetaStoreClientSupplier hiveMetaStoreClientSupplier;
  private @Mock HiveConf localHiveConf;
  private HiveConf hiveConf;
  private TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory;
  private @Mock HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory;

  private final String metastoreUri = "thrift://metastore-host:42";
  private final String name = "test";
  private final int reconnectionRetries = 10;
  private final String localHost = "my-machine";

  @Before
  public void init() {
    hiveConf = metastore.conf();
    when(tunnelableFactorySupplier.get(any(HiveConf.class))).thenReturn(tunnelableFactory);
    when(localHiveConfFactory.newInstance(any(String.class), any(Integer.class), eq(hiveConf)))
        .thenReturn(localHiveConf);
    when(localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS)).thenReturn(metastoreUri);
    when(hiveMetaStoreClientSupplierFactory.newInstance(localHiveConf, name, reconnectionRetries))
        .thenReturn(hiveMetaStoreClientSupplier);
    hiveConf.set(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, "private_key");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname, "");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_STRICT_HOST_KEY_CHECKING.varname, "yes");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_ROUTE.varname, "user@hop1 -> hop2");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname, localHost);

    tunnelingMetaStoreClientFactory = new TunnelingMetaStoreClientFactory(tunnelableFactorySupplier,
        metaStoreClientFactory, localHiveConfFactory, hiveMetaStoreClientSupplierFactory);
  }

  @Test
  public void newInstanceWithTunneling() throws Exception {
    ArgumentCaptor<TunnelableSupplier> captor = ArgumentCaptor.forClass(TunnelableSupplier.class);
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, reconnectionRetries);
    verify(tunnelableFactory)
        .wrap(captor.capture(), eq(tunnelingMetaStoreClientFactory.METHOD_CHECKER), eq("my-machine"), anyInt(),
            eq("localhost"), eq(metastore.getThriftPort()));
  }

  @Test
  public void localHiveConfigUsesCorrectParameters() {
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, reconnectionRetries);
    ArgumentCaptor<String> stringArgument = ArgumentCaptor.forClass(String.class);
    verify(localHiveConfFactory).newInstance(stringArgument.capture(), anyInt(), eq(hiveConf));
    assertThat(stringArgument.getValue(), is(localHost));
  }

  @Test
  public void tunnelableFactorySupplierUsesCorrectParameters() {
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, reconnectionRetries);
    verify(tunnelableFactorySupplier).get(eq(localHiveConf));
  }

  @Test
  public void hiveMetaStoreClientSupplierFactoryUsesCorrectParameters() {
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, reconnectionRetries);
    verify(hiveMetaStoreClientSupplierFactory).newInstance(eq(localHiveConf), eq(name), eq(reconnectionRetries));
  }

  @Test
  public void tunnelableFactoryUsesCorrectParameters() throws URISyntaxException {
    URI uri = new URI(metastore.getThriftConnectionUri());
    String remoteHost = uri.getHost();
    int remotePort = uri.getPort();
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, reconnectionRetries);
    verify(tunnelableFactory)
        .wrap(eq(hiveMetaStoreClientSupplier), eq(tunnelingMetaStoreClientFactory.METHOD_CHECKER), eq(localHost),
            any(Integer.class), eq(remoteHost), eq(remotePort));
  }

  @Test
  public void newInstanceWithoutTunneling() throws Exception {
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_ROUTE.varname);
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname);
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, reconnectionRetries);
    verify(metaStoreClientFactory).newInstance(hiveConf, name, reconnectionRetries);
    verifyZeroInteractions(tunnelableFactorySupplier);
  }

  @Test
  public void newInstanceWithoutTunnelingNorSupplier() throws ConfigValSecurityException, TException {
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_ROUTE.varname);
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname);

    TunnelingMetaStoreClientFactory clientFactory = new TunnelingMetaStoreClientFactory();
    CloseableThriftHiveMetastoreIface closeableThriftHiveMetastoreIface = clientFactory
        .newInstance(hiveConf, name, reconnectionRetries);
    assertNotNull(closeableThriftHiveMetastoreIface);
  }

}
