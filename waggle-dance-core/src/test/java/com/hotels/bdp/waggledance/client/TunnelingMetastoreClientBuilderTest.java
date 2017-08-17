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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
@PrepareForTest(TunnelingMetastoreClientBuilder.class)
public class TunnelingMetastoreClientBuilderTest {
  private @Spy TunnelingMetastoreClientBuilder tunnelingMetastoreClientBuilder = new TunnelingMetastoreClientBuilder();
  private @Mock TunnelConnectionManagerFactory tunnelConnectionManagerFactory;
  private @Spy HiveConf hiveConf = new HiveConf();
  private @Mock TunnelConnectionManager tunnelConnectionManager;
  private @Mock Tunnel tunnel;

  @Test
  public void build() throws Exception {
    hiveConf.set(WaggleDanceHiveConfVars.SSH_ROUTE.varname, "user@ec2-12-345-678-91.compute-1.amazonaws.com");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, "private_key");
    String originalMetastoreUri = "thrift://internal-test-shared-hive-metastore-elb-1234567891.us-west-1.elb.amazonaws.com:1234";
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, originalMetastoreUri);

    when(tunnelConnectionManagerFactory.create(anyString(), anyString(), anyInt(), anyString(), anyInt())).thenReturn(
        tunnelConnectionManager);
    when(tunnelConnectionManager.getTunnel(anyString(), anyInt())).thenReturn(tunnel);
    doNothing().when(tunnelConnectionManager).open();

    tunnelingMetastoreClientBuilder
        .setHiveConf(hiveConf)
        .setName("name")
        .setReconnectionRetries(10)
        .setTunnelConnectionManagerFactory(tunnelConnectionManagerFactory)
        .setRemoteHost("remote")
        .setSSHRoute("route")
        .setLocalHost("local")
        .setRemotePort(9083)
        .build();

    verify(tunnelConnectionManager, times(2)).getTunnel(anyString(), anyInt());
    verify(tunnelConnectionManager).open();

    //Check that Client is being created from a hiveConf different to that passed into the builder
    PowerMockito.verifyPrivate(tunnelingMetastoreClientBuilder, times(0))
                .invoke("clientFromLocalHiveConf", tunnelConnectionManager, hiveConf);
    PowerMockito.verifyPrivate(tunnelingMetastoreClientBuilder, times(1))
                .invoke("clientFromLocalHiveConf", eq(tunnelConnectionManager), any(HiveConf.class));
  }

  @Test
  public void setHiveConf() throws Exception {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "test");
    tunnelingMetastoreClientBuilder = tunnelingMetastoreClientBuilder.setHiveConf(hiveConf);
    assertEquals(hiveConf, tunnelingMetastoreClientBuilder.getHiveConf());
  }

  @Test
  public void setName() throws Exception {
    String name = "name";
    tunnelingMetastoreClientBuilder = tunnelingMetastoreClientBuilder.setName(name);
    assertEquals(name, tunnelingMetastoreClientBuilder.getName());
  }

  @Test
  public void setReconnectionRetries() throws Exception {
    Integer retries = 5;
    tunnelingMetastoreClientBuilder = tunnelingMetastoreClientBuilder.setReconnectionRetries(retries);
    assertEquals(retries, tunnelingMetastoreClientBuilder.getReconnectionRetries());
  }

  @Test
  public void setTunnelConnectionManagerFactory() throws Exception {
    tunnelingMetastoreClientBuilder = tunnelingMetastoreClientBuilder.setTunnelConnectionManagerFactory(
        tunnelConnectionManagerFactory);
    assertEquals(tunnelConnectionManagerFactory, tunnelingMetastoreClientBuilder.getTunnelConnectionManagerFactory());
  }

  @Test
  public void setRemoteHost() throws Exception {
    String host = "host";
    tunnelingMetastoreClientBuilder = tunnelingMetastoreClientBuilder.setRemoteHost(host);
    assertEquals(host, tunnelingMetastoreClientBuilder.getRemoteHost());
  }

  @Test
  public void setSSHRoute() throws Exception {
    String route = "route";
    tunnelingMetastoreClientBuilder = tunnelingMetastoreClientBuilder.setSSHRoute(route);
    assertEquals(route, tunnelingMetastoreClientBuilder.getSSHRoute());
  }

  @Test
  public void setLocalHost() throws Exception {
    String host = "host";
    tunnelingMetastoreClientBuilder = tunnelingMetastoreClientBuilder.setLocalHost(host);
    assertEquals(host, tunnelingMetastoreClientBuilder.getLocalHost());
  }

  @Test
  public void setRemotePort() throws Exception {
    Integer port = 9083;
    tunnelingMetastoreClientBuilder = tunnelingMetastoreClientBuilder.setRemotePort(port);
    assertEquals(port, tunnelingMetastoreClientBuilder.getRemotePort());
  }
}