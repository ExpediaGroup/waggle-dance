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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.pastdev.jsch.tunnel.Tunnel;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

@RunWith(MockitoJUnitRunner.class)
public class TunnelingMetaStoreClientFactoryTest {

  private @Mock CloseableThriftHiveMetastoreIface client;
  private @Mock TunnelConnectionManager tunnelConnectionManager;
  private @Mock Tunnel tunnel;
  private @Mock TunnelHandler tunnelHandler;
  private @Mock TunnelConnectionManagerFactory tunnelConnectionManagerFactory;
  private final HiveConf hiveConf = new HiveConf();
  private TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory;

  @Before
  public void init() {
    tunnelingMetaStoreClientFactory = spy(new TunnelingMetaStoreClientFactory(tunnelHandler));
    when(tunnelConnectionManager.getTunnel(anyString(), anyInt())).thenReturn(tunnel);
    when(tunnelConnectionManagerFactory.create(anyString(), anyString(), anyInt(), anyString(), anyInt()))
        .thenReturn(tunnelConnectionManager);

    hiveConf.set(WaggleDanceHiveConfVars.SSH_ROUTE.varname, "user@ec2-12-345-678-91.compute-1.amazonaws.com");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, "private_key");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname, "");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,
        "thrift://internal-test-shared-hive-metastore-elb-1234567891.us-west-1.elb.amazonaws.com:1234");
  }

  @Test
  public void newInstanceWithTunneling() throws Exception {
    doReturn(tunnelConnectionManagerFactory).when(tunnelingMetaStoreClientFactory).createTunnelConnectionManagerFactory(
        any(HiveConf.class));
    tunnelingMetaStoreClientFactory.newInstance(hiveConf, "test", 10);
    verify(tunnelHandler).openTunnel(eq(tunnelConnectionManager), anyString(), anyString(), anyString(), anyInt());
  }

  @Test
  public void newInstanceWithoutTunneling() throws Exception {
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname);
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_ROUTE.varname);
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname);
    doReturn(tunnelConnectionManagerFactory).when(tunnelingMetaStoreClientFactory).createTunnelConnectionManagerFactory(
        any(HiveConf.class));
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,
        "thrift://internal-foo-baz-metastore-1234567891.made-up-region-1.elb.amazonaws.com:1234");

    tunnelingMetaStoreClientFactory.newInstance(hiveConf, "test", 10);
    verifyZeroInteractions(tunnelHandler);
  }

  @Test
  public void highPort() {
    hiveConf.setInt(WaggleDanceHiveConfVars.SSH_PORT.varname, 65535);
    tunnelingMetaStoreClientFactory.createTunnelConnectionManagerFactory(hiveConf);
  }

  @Test
  public void nullKnownHosts() {
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname);
    tunnelingMetaStoreClientFactory.createTunnelConnectionManagerFactory(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativePort() {
    hiveConf.setInt(WaggleDanceHiveConfVars.SSH_PORT.varname, -1);
    tunnelingMetaStoreClientFactory.createTunnelConnectionManagerFactory(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroPort() {
    hiveConf.setInt(WaggleDanceHiveConfVars.SSH_PORT.varname, 0);
    tunnelingMetaStoreClientFactory.createTunnelConnectionManagerFactory(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void tooHighPort() {
    hiveConf.setInt(WaggleDanceHiveConfVars.SSH_PORT.varname, 65537);
    tunnelingMetaStoreClientFactory.createTunnelConnectionManagerFactory(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullKeys() {
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname);
    tunnelingMetaStoreClientFactory.createTunnelConnectionManagerFactory(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyKeys() {
    hiveConf.set(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, "");
    tunnelingMetaStoreClientFactory.createTunnelConnectionManagerFactory(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void incorrectStrictHostKeyCheckingSetting() {
    hiveConf.set(WaggleDanceHiveConfVars.SSH_STRICT_HOST_KEY_CHECKING.varname, "foo");
    tunnelingMetaStoreClientFactory.createTunnelConnectionManagerFactory(hiveConf);
  }
}
