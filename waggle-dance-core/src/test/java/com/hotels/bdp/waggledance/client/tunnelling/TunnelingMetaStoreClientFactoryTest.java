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
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;
import com.hotels.hcommon.ssh.TunnelableFactory;

@RunWith(MockitoJUnitRunner.class)
public class TunnelingMetaStoreClientFactoryTest {

  public @Rule ThriftHiveMetaStoreJUnitRule metastore = new ThriftHiveMetaStoreJUnitRule();

  private @Captor ArgumentCaptor<String> hostCaptor;
  private @Mock HiveMetaStoreClientSupplier hiveMetaStoreClientSupplier;
  private @Mock HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory;
  private @Mock TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory;
  private @Mock CloseableThriftHiveMetastoreIface expectedTunnelable;
  private @Mock LocalHiveConfFactory localHiveConfFactory;
  private HiveConf hiveConf;
  private final HiveConf localHiveConf = new HiveConf();
  private TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory;

  private final String localMetastoreUri = "thrift://my-machine:123";
  private final String name = "test";
  private final int retries = 10;
  private final String localhost = "my-machine";
  private int remotePort;

  @Before
  public void init() {
    hiveConf = metastore.conf();
    remotePort = metastore.getThriftPort();
    localHiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, localMetastoreUri);
    when(hiveMetaStoreClientSupplierFactory.newInstance(any(), eq(name), eq(retries)))
        .thenReturn(hiveMetaStoreClientSupplier);
    when(localHiveConfFactory.newInstance(eq(localhost), anyInt(), eq(hiveConf))).thenReturn(localHiveConf);

    tunnelingMetaStoreClientFactory = new TunnelingMetaStoreClientFactory(hiveMetaStoreClientSupplierFactory,
        localHiveConfFactory, tunnelableFactory, localhost);
  }

  @Test
  public void newInstanceWithTunneling() {
    when(tunnelableFactory
        .wrap(eq(hiveMetaStoreClientSupplier), eq(tunnelingMetaStoreClientFactory.METHOD_CHECKER), eq(localhost),
            anyInt(), eq("localhost"), eq(remotePort))).thenReturn(expectedTunnelable);
    CloseableThriftHiveMetastoreIface result = tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, retries);
    assertThat(result, is(expectedTunnelable));
  }

  @Test
  public void newInstanceWithTunnelingAndLocalhost() {
    tunnelingMetaStoreClientFactory = new TunnelingMetaStoreClientFactory(tunnelableFactory, localhost);
    when(tunnelableFactory
        .wrap(any(HiveMetaStoreClientSupplier.class), eq(tunnelingMetaStoreClientFactory.METHOD_CHECKER), eq(localhost),
            anyInt(), eq("localhost"), eq(remotePort))).thenReturn(expectedTunnelable);
    CloseableThriftHiveMetastoreIface result = tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, retries);
    assertThat(result, is(expectedTunnelable));
  }

  @Test(expected = IllegalStateException.class)
  public void newInstanceNoTunnelableFactorySet() {
    tunnelingMetaStoreClientFactory = new TunnelingMetaStoreClientFactory(hiveMetaStoreClientSupplierFactory,
        localHiveConfFactory, null, localhost);
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, retries);
  }

  @Test(expected = IllegalStateException.class)
  public void newInstanceNoLocalHostSet() {
    tunnelingMetaStoreClientFactory = new TunnelingMetaStoreClientFactory(hiveMetaStoreClientSupplierFactory,
        localHiveConfFactory, tunnelableFactory, null);
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, retries);
  }

  @Test
  public void localHiveConfigUsesCorrectParameters() {
    when(localHiveConfFactory.newInstance(hostCaptor.capture(), anyInt(), eq(hiveConf))).thenReturn(localHiveConf);
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, retries);
    assertThat(hostCaptor.getValue(), is(localhost));
  }

  @Test
  public void hiveMetaStoreClientSupplierFactoryUsesCorrectParameters() {
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, retries);
    ArgumentCaptor<HiveConf> hiveConfCaptor = ArgumentCaptor.forClass(HiveConf.class);
    verify(hiveMetaStoreClientSupplierFactory).newInstance(hiveConfCaptor.capture(), eq(name), eq(retries));
    assertThat(hiveConfCaptor.getValue(), is(localHiveConf));
  }

  @Test
  public void tunnelableFactoryUsesCorrectParameters() throws URISyntaxException {
    URI uri = new URI(metastore.getThriftConnectionUri());
    String remoteHost = uri.getHost();
    int remotePort = uri.getPort();
    when(tunnelableFactory
        .wrap(eq(hiveMetaStoreClientSupplier), eq(tunnelingMetaStoreClientFactory.METHOD_CHECKER), eq(localhost),
            anyInt(), eq(remoteHost), eq(remotePort))).thenReturn(expectedTunnelable);
    CloseableThriftHiveMetastoreIface result = tunnelingMetaStoreClientFactory.newInstance(hiveConf, name, retries);
    assertThat(result, is(expectedTunnelable));
  }

}
