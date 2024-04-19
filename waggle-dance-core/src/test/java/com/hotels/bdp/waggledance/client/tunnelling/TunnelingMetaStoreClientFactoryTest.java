/**
 * Copyright (C) 2016-2024 Expedia, Inc.
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

  private static final int METASTORE_PORT = 42;
  private static final int RECONNECTION_RETRIES = 10;
  private static final int CONNECTION_TIMEOUT = 10;
  private static final String METASTORE_HOST = "metastore-host";
  private static final String METASTORE_URI = "thrift://" + METASTORE_HOST + ":" + METASTORE_PORT;
  private static final String NAME = "test";
  private static final String TUNNEL_ROUTE = "user@hop1 -> hop2";
  private static final String TUNNEL_PRIVATE_KEY = "private_key";
  private static final String TUNNEL_KNOWN_HOSTS = "known_hosts";
  private static final String TUNNEL_LOCALHOST = "my-machine";

  private @Mock TunnelableFactorySupplier tunnelableFactorySupplier;
  private @Mock TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory;
  private @Mock LocalHiveConfFactory localHiveConfFactory;
  private @Mock HiveMetaStoreClientSupplier hiveMetaStoreClientSupplier;
  private @Mock HiveConf localHiveConf;
  private @Mock HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory;
  private @Captor ArgumentCaptor<TunnelableSupplier<CloseableThriftHiveMetastoreIface>> tunnelableSupplierCaptor;
  private MetastoreTunnel metastoreTunnel;
  private TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory;
  private Map<String, String> waggleDanceConfigurationProperties = new HashMap<>();

  @Before
  public void init() {
    metastoreTunnel = new MetastoreTunnel();
    metastoreTunnel.setRoute(TUNNEL_ROUTE);
    metastoreTunnel.setPrivateKeys(TUNNEL_PRIVATE_KEY);
    metastoreTunnel.setKnownHosts(TUNNEL_KNOWN_HOSTS);
    metastoreTunnel.setLocalhost(TUNNEL_LOCALHOST);

    waggleDanceConfigurationProperties.put(ConfVars.METASTORETHRIFTCONNECTIONRETRIES.varname, "5");
    waggleDanceConfigurationProperties.put(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.varname, "6");
    waggleDanceConfigurationProperties.put(ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY.varname, "5");
    waggleDanceConfigurationProperties.put(ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT.varname, "true");
    waggleDanceConfigurationProperties.put(ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL.varname, "false");

    when(localHiveConfFactory.newInstance(any(String.class), any(Integer.class), any(HiveConf.class)))
        .thenReturn(localHiveConf);
    when(tunnelableFactorySupplier.get(metastoreTunnel)).thenReturn(tunnelableFactory);
    when(localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS)).thenReturn(METASTORE_URI);
    when(hiveMetaStoreClientSupplierFactory.newInstance(localHiveConf, NAME, RECONNECTION_RETRIES, CONNECTION_TIMEOUT))
        .thenReturn(hiveMetaStoreClientSupplier);

    tunnelingMetaStoreClientFactory = new TunnelingMetaStoreClientFactory(tunnelableFactorySupplier,
        localHiveConfFactory, hiveMetaStoreClientSupplierFactory);
  }

  @Test
  public void newInstance() {
    tunnelingMetaStoreClientFactory.newInstance(METASTORE_URI, metastoreTunnel, NAME, RECONNECTION_RETRIES,
        CONNECTION_TIMEOUT, waggleDanceConfigurationProperties);
    verify(tunnelableFactory)
        .wrap(tunnelableSupplierCaptor.capture(), eq(tunnelingMetaStoreClientFactory.METHOD_CHECKER),
            eq(metastoreTunnel.getLocalhost()), anyInt(), eq(METASTORE_HOST), eq(METASTORE_PORT));
    TunnelableSupplier<CloseableThriftHiveMetastoreIface> tunnelable = tunnelableSupplierCaptor.getValue();
    assertThat(tunnelable, is(hiveMetaStoreClientSupplier));
  }

  @Test
  public void newInstanceNullConfigurationProperties() {
    tunnelingMetaStoreClientFactory.newInstance(METASTORE_URI, metastoreTunnel, NAME, RECONNECTION_RETRIES,
        CONNECTION_TIMEOUT, null);
  }

  @Test
  public void newInstanceMultipleUris() {
    String metastoreUris = METASTORE_URI + ",thrift://metastore-host2:43";
    tunnelingMetaStoreClientFactory.newInstance(metastoreUris, metastoreTunnel, NAME, RECONNECTION_RETRIES,
        CONNECTION_TIMEOUT, waggleDanceConfigurationProperties);
    verify(tunnelableFactory)
        .wrap(tunnelableSupplierCaptor.capture(), eq(tunnelingMetaStoreClientFactory.METHOD_CHECKER),
            eq(metastoreTunnel.getLocalhost()), anyInt(), eq(METASTORE_HOST), eq(METASTORE_PORT));
    TunnelableSupplier<CloseableThriftHiveMetastoreIface> tunnelable = tunnelableSupplierCaptor.getValue();
    assertThat(tunnelable, is(hiveMetaStoreClientSupplier));
  }

  @Test
  public void localHiveConfigUsesCorrectParameters() {
    tunnelingMetaStoreClientFactory.newInstance(METASTORE_URI, metastoreTunnel, NAME, RECONNECTION_RETRIES,
        CONNECTION_TIMEOUT, waggleDanceConfigurationProperties);
    ArgumentCaptor<String> localHostCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<HiveConf> hiveConfCaptor = ArgumentCaptor.forClass(HiveConf.class);
    // we get random assigned free port for local port
    verify(localHiveConfFactory).newInstance(localHostCaptor.capture(), anyInt(), hiveConfCaptor.capture());
    assertThat(localHostCaptor.getValue(), is(TUNNEL_LOCALHOST));
    HiveConf hiveConf = hiveConfCaptor.getValue();
    assertThat(hiveConf.get(ConfVars.METASTOREURIS.varname), is(METASTORE_URI));
    assertThat(hiveConf.getIntVar(ConfVars.METASTORETHRIFTCONNECTIONRETRIES), is(5));
    assertThat(hiveConf.getTimeVar(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS), is(6000L));
    assertThat(hiveConf.getTimeVar(ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS), is(5L));
    assertThat(hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT), is(true));
    assertThat(hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL), is(false));
  }
}
