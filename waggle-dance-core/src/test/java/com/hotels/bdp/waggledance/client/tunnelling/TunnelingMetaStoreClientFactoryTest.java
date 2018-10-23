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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;
import com.hotels.hcommon.ssh.TunnelableFactory;
import com.hotels.hcommon.ssh.TunnelableSupplier;

@RunWith(MockitoJUnitRunner.class)
public class TunnelingMetaStoreClientFactoryTest {

  private static final String TUNNEL_ROUTE = "user@hop1 -> hop2";
  private static final String TUNNEL_PRIVATE_KEY = "private_key";
  private static final String TUNNEL_KNOWN_HOSTS = "known_hosts";
  private static final String TUNNEL_LOCALHOST = "my-machine";

  private @Mock TunnelableFactorySupplier tunnelableFactorySupplier;
  private @Mock TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory;
  private @Mock LocalHiveConfFactory localHiveConfFactory;
  private @Mock HiveMetaStoreClientSupplier hiveMetaStoreClientSupplier;
  private @Mock HiveConf localHiveConf;
  private MetastoreTunnel metastoreTunnel;
  private TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory;
  private @Mock HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory;
  private @Captor ArgumentCaptor<TunnelableSupplier<CloseableThriftHiveMetastoreIface>> tunnelableSupplierCaptor;

  private final String metastoreUri = "thrift://metastore-host:42";
  private final String name = "test";
  private final int reconnectionRetries = 10;

  @Before
  public void init() {
    metastoreTunnel = new MetastoreTunnel();
    metastoreTunnel.setRoute(TUNNEL_ROUTE);
    metastoreTunnel.setPrivateKeys(TUNNEL_PRIVATE_KEY);
    metastoreTunnel.setKnownHosts(TUNNEL_KNOWN_HOSTS);
    metastoreTunnel.setLocalhost(TUNNEL_LOCALHOST);

    when(localHiveConfFactory.newInstance(any(String.class), any(Integer.class), any(HiveConf.class)))
        .thenReturn(localHiveConf);
    when(tunnelableFactorySupplier.get(metastoreTunnel)).thenReturn(tunnelableFactory);
    when(localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS)).thenReturn(metastoreUri);
    when(hiveMetaStoreClientSupplierFactory.newInstance(localHiveConf, name, reconnectionRetries))
        .thenReturn(hiveMetaStoreClientSupplier);

    tunnelingMetaStoreClientFactory = new TunnelingMetaStoreClientFactory(tunnelableFactorySupplier,
        localHiveConfFactory, hiveMetaStoreClientSupplierFactory);
  }

  @Test
  public void newInstance() throws Exception {
    tunnelingMetaStoreClientFactory.newInstance(metastoreUri, metastoreTunnel, name, reconnectionRetries);
    verify(tunnelableFactory)
        .wrap(tunnelableSupplierCaptor.capture(), eq(tunnelingMetaStoreClientFactory.METHOD_CHECKER),
            eq(metastoreTunnel.getLocalhost()), anyInt(), eq("metastore-host"), eq(42));
    TunnelableSupplier<CloseableThriftHiveMetastoreIface> tunnelable = tunnelableSupplierCaptor.getValue();
    assertThat(tunnelable, is(hiveMetaStoreClientSupplier));
  }

  @Test
  public void newInstanceMultipleUris() throws Exception {
    String metastoreUris = metastoreUri + ",thrift://metastore-host2:43";
    tunnelingMetaStoreClientFactory.newInstance(metastoreUris, metastoreTunnel, name, reconnectionRetries);
    verify(tunnelableFactory)
        .wrap(tunnelableSupplierCaptor.capture(), eq(tunnelingMetaStoreClientFactory.METHOD_CHECKER),
            eq(metastoreTunnel.getLocalhost()), anyInt(), eq("metastore-host"), eq(42));
    TunnelableSupplier<CloseableThriftHiveMetastoreIface> tunnelable = tunnelableSupplierCaptor.getValue();
    assertThat(tunnelable, is(hiveMetaStoreClientSupplier));
  }

  @Test
  public void localHiveConfigUsesCorrectParameters() {
    tunnelingMetaStoreClientFactory.newInstance(metastoreUri, metastoreTunnel, name, reconnectionRetries);
    ArgumentCaptor<String> localHostCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<HiveConf> hiveConfCaptor = ArgumentCaptor.forClass(HiveConf.class);
    // we get random assigned free port for local port
    verify(localHiveConfFactory).newInstance(localHostCaptor.capture(), anyInt(), hiveConfCaptor.capture());
    assertThat(localHostCaptor.getValue(), is(TUNNEL_LOCALHOST));
    HiveConf hiveConf = hiveConfCaptor.getValue();
    assertThat(hiveConf.get(ConfVars.METASTOREURIS.varname), is(metastoreUri));
  }
}
