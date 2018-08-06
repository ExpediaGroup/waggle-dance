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
package com.hotels.bdp.waggledance.client.tunneling;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

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
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;
import com.hotels.hcommon.ssh.TunnelableFactory;
import com.hotels.hcommon.ssh.TunnelableSupplier;

@RunWith(MockitoJUnitRunner.class)
public class TunnelingMetaStoreClientFactoryTest {

  public @Rule ThriftHiveMetaStoreJUnitRule metastore = new ThriftHiveMetaStoreJUnitRule();

  private @Mock TunnelableFactorySupplier tunnelableFactorySupplier;
  private @Mock MetaStoreClientFactory metaStoreClientFactory;
  private @Mock TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory;
  private @Mock HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory;
  private @Mock LocalHiveConfFactory localHiveConfFactory;
  private @Mock HiveConf localHiveConf;
  private HiveConf hiveConf;
  private TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory;

  private final String metastoreUri = "thrift://host:30";

  @Before
  public void init() {
    hiveConf = metastore.conf();
    when(tunnelableFactorySupplier.get(any(HiveConf.class))).thenReturn(tunnelableFactory);
    hiveConf.set(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, "private_key");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname, "");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_STRICT_HOST_KEY_CHECKING.varname, "yes");
    tunnelingMetaStoreClientFactory = new TunnelingMetaStoreClientFactory(tunnelableFactorySupplier,
        metaStoreClientFactory, localHiveConfFactory, hiveMetaStoreClientSupplierFactory);

    when(localHiveConfFactory.createLocalHiveConf(any(String.class), any(Integer.class), eq(hiveConf)))
        .thenReturn(localHiveConf);
    when(localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS)).thenReturn(metastoreUri);
  }

  @Test
  public void newInstanceWithTunneling() throws Exception {
    ArgumentCaptor<TunnelableSupplier> captor = ArgumentCaptor.forClass(TunnelableSupplier.class);
    hiveConf.set(WaggleDanceHiveConfVars.SSH_ROUTE.varname, "user@hop1 -> hop2");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname, "my-machine");
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, "test", 10);
    verify(tunnelableFactory)
        .wrap(captor.capture(), eq(tunnelingMetaStoreClientFactory.METHOD_CHECKER), eq("my-machine"), anyInt(),
            eq("localhost"), eq(metastore.getThriftPort()));

    // Verify that localHiveConf is created with correct parameters
    ArgumentCaptor<String> stringArgument = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> intArgument = ArgumentCaptor.forClass(Integer.class);
    String remotePort = hiveConf.get(HiveConf.ConfVars.METASTORE_SERVER_PORT.varname);
    verify(localHiveConfFactory).createLocalHiveConf(stringArgument.capture(), intArgument.capture(), eq(hiveConf));
    assertThat(stringArgument.getValue(), is(hiveConf.get(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname)));
    assertFalse(String.valueOf(intArgument.getValue()) == remotePort);

    // Test tunnelableFactorySupplier.get(...) with localHiveConf and not hiveConf
    verify(tunnelableFactorySupplier).get(eq(localHiveConf));
  }

  @Test
  public void newInstanceWithoutTunneling() throws Exception {
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, "test", 10);
    verify(metaStoreClientFactory).newInstance(hiveConf, "test", 10);
    verifyZeroInteractions(tunnelableFactorySupplier);
  }

  @Test
  public void newInstanceWithoutTunnelingNorSupplier() throws ConfigValSecurityException, TException {
    TunnelingMetaStoreClientFactory clientFactory = new TunnelingMetaStoreClientFactory();
    CloseableThriftHiveMetastoreIface closeableThriftHiveMetastoreIface = clientFactory
        .newInstance(hiveConf, "test", 10);
    assertNotNull(closeableThriftHiveMetastoreIface);
  }

}
