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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.client.TunnelingMetaStoreClientFactory.METHOD_CHECKER;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;
import com.hotels.hcommon.ssh.TunnelableFactory;
import com.hotels.hcommon.ssh.TunnelableSupplier;

@RunWith(MockitoJUnitRunner.class)
public class TunnelingMetaStoreClientFactoryTest {

  public @Rule ThriftHiveMetaStoreJUnitRule metastore = new ThriftHiveMetaStoreJUnitRule();

  private @Mock TunnelableFactorySupplier tunnelableFactorySupplier;
  private @Mock MetaStoreClientFactory metaStoreClientFactory;
  private @Mock TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory;
  private HiveConf hiveConf;
  private TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory;

  @Before
  public void init() {
    hiveConf = metastore.conf();
    when(tunnelableFactorySupplier.get(any(HiveConf.class))).thenReturn(tunnelableFactory);
    hiveConf.set(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, "private_key");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname, "");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_STRICT_HOST_KEY_CHECKING.varname, "yes");
    tunnelingMetaStoreClientFactory = new TunnelingMetaStoreClientFactory(tunnelableFactorySupplier,
        metaStoreClientFactory);
  }

  @Test
  public void newInstanceWithTunneling() throws Exception {
    ArgumentCaptor<TunnelableSupplier> captor = ArgumentCaptor.forClass(TunnelableSupplier.class);
    hiveConf.set(WaggleDanceHiveConfVars.SSH_ROUTE.varname, "user@hop1 -> hop2");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname, "my-machine");
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, "test", 10);
    verify(tunnelableFactory)
        .wrap(captor.capture(), eq(METHOD_CHECKER), eq("my-machine"), anyInt(), eq("localhost"),
            eq(metastore.getThriftPort()));
  }

  @Test
  public void newInstanceWithoutTunneling() throws Exception {
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, "test", 10);
    verify(metaStoreClientFactory).newInstance(hiveConf, "test", 10);
    verifyZeroInteractions(tunnelableFactorySupplier);
  }

  @Test
  public void newInstanceWithoutTunnelingNorSupplier() {
    TunnelingMetaStoreClientFactory clientFactory = new TunnelingMetaStoreClientFactory();
    clientFactory.newInstance(hiveConf, "test", 10);
  }

}
