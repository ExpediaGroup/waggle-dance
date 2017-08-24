/**
 * Copyright (C) 2016-2017 Expedia Inc.
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

import static org.mockito.AdditionalMatchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.when;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.pastdev.jsch.tunnel.Tunnel;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TunnelHandler.class, TunnelingMetaStoreClientFactory.class })
public class TunnelingMetaStoreClientFactoryTest {

  private @Spy TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory = new TunnelingMetaStoreClientFactory();
  private @Mock CloseableThriftHiveMetastoreIface client;
  private @Mock TunnelConnectionManager tunnelConnectionManager;
  private @Mock Tunnel tunnel;
  private @Spy HiveConf hiveConf = new HiveConf();
  private @Spy HiveConf localHiveConf = new HiveConf();

  @Test
  public void newInstanceWithTunneling() throws Exception {
    hiveConf.set(WaggleDanceHiveConfVars.SSH_ROUTE.varname, "user@ec2-12-345-678-91.compute-1.amazonaws.com");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, "private_key");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,
        "thrift://internal-test-shared-hive-metastore-elb-1234567891.us-west-1.elb.amazonaws.com:1234");

    PowerMockito.mockStatic(TunnelHandler.class);
    PowerMockito.doNothing()
                .when(TunnelHandler.class, "openTunnel", any(TunnelConnectionManager.class), anyString(), anyString(),
                    anyString(), anyInt());
    PowerMockito.doReturn(tunnelConnectionManager)
                .when(tunnelingMetaStoreClientFactory, "createTunnelConnectionManager");
    PowerMockito.doReturn(client)
                .when(tunnelingMetaStoreClientFactory, "createTunnelingMetastoreClient", eq(localHiveConf), anyString(),
                    anyInt());

    when(tunnelConnectionManager.getTunnel(anyString(), anyInt())).thenReturn(tunnel);
    when(tunnel.getAssignedLocalPort()).thenReturn(123);

    tunnelingMetaStoreClientFactory.newInstance(hiveConf, "test", 10);

    PowerMockito.verifyPrivate(tunnelingMetaStoreClientFactory, times(1)).invoke("createTunnelConnectionManager");
    PowerMockito.verifyPrivate(tunnelingMetaStoreClientFactory, times(1)).invoke("createLocalHiveConf");
    PowerMockito.verifyPrivate(tunnelingMetaStoreClientFactory, times(1))
                .invoke("createTunnelingMetastoreClient", not(eq(hiveConf)), anyString(), anyInt());
  }

  @Test
  public void newInstanceWithoutTunneling() throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,
        "thrift://internal-foo-baz-metastore-1234567891.made-up-region-1.elb.amazonaws.com:1234");

    PowerMockito.verifyPrivate(tunnelingMetaStoreClientFactory, times(0)).invoke("createTunnelConnectionManager");
    PowerMockito.verifyPrivate(tunnelingMetaStoreClientFactory, times(0))
                .invoke("createTunnelingMetastoreClient", eq(localHiveConf), anyString(), anyInt());
  }
}