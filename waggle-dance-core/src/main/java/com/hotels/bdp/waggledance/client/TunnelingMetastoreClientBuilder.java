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

import static com.hotels.bdp.waggledance.client.MetaStoreClientFactory.INTERFACES;
import static com.hotels.bdp.waggledance.client.TunnelConnectionManagerFactory.FIRST_AVAILABLE_PORT;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.hadoop.hive.conf.HiveConf;
import org.springframework.stereotype.Component;

import com.pastdev.jsch.tunnel.TunnelConnectionManager;

@Component
public class TunnelingMetastoreClientBuilder {

  private HiveConf hiveConf = new HiveConf();
  private String name = "";
  private Integer reconnectionRetries = 1;
  private TunnelConnectionManagerFactory tunnelConnectionManagerFactory = null;
  private String remoteHost = "thrift://localhost:9083";
  private String sshRoute = "";
  private String localHost = "thrift://localhost:9083";
  private Integer remotePort = 9083;

  public CloseableThriftHiveMetastoreIface build() {
    TunnelConnectionManager tunnelConnectionManager = tunnelConnectionManagerFactory.create(sshRoute, localHost,
        FIRST_AVAILABLE_PORT, remoteHost, remotePort);
    WaggleDanceTunnel tunnel = new WaggleDanceTunnel(hiveConf, tunnelConnectionManager, sshRoute, localHost, remoteHost,
        remotePort);
    HiveConf localHiveConf = tunnel.create();
    return new TunnelingMetastoreClientInvocationHandler(tunnelConnectionManager,
        new MetaStoreClientFactory().newInstance(localHiveConf, name, reconnectionRetries)).newInstance();
  }

  public TunnelingMetastoreClientBuilder withHiveConf(HiveConf hiveConf) {
    this.hiveConf = new HiveConf(hiveConf);
    return this;
  }

  public TunnelingMetastoreClientBuilder withName(String name) {
    this.name = name;
    return this;
  }

  public TunnelingMetastoreClientBuilder withReconnectionRetries(Integer reconnectionRetries) {
    this.reconnectionRetries = reconnectionRetries;
    return this;
  }

  public TunnelingMetastoreClientBuilder withTunnelConnectionManagerFactory(
      TunnelConnectionManagerFactory tunnelConnectionManagerFactory) {
    this.tunnelConnectionManagerFactory = tunnelConnectionManagerFactory;
    return this;
  }

  public TunnelingMetastoreClientBuilder withRemoteHost(String remoteHost) {
    this.remoteHost = remoteHost;
    return this;
  }

  public TunnelingMetastoreClientBuilder withSSHRoute(String sshRoute) {
    this.sshRoute = sshRoute;
    return this;
  }

  public TunnelingMetastoreClientBuilder withLocalHost(String localHost) {
    this.localHost = localHost;
    return this;
  }

  public TunnelingMetastoreClientBuilder withRemotePort(Integer remotePort) {
    this.remotePort = remotePort;
    return this;
  }

  public HiveConf getHiveConf() {
    return hiveConf;
  }

  public String getName() {
    return name;
  }

  public Integer getReconnectionRetries() {
    return reconnectionRetries;
  }

  public TunnelConnectionManagerFactory getTunnelConnectionManagerFactory() {
    return tunnelConnectionManagerFactory;
  }

  public String getRemoteHost() {
    return remoteHost;
  }

  public String getSSHRoute() {
    return sshRoute;
  }

  public String getLocalHost() {
    return localHost;
  }

  public Integer getRemotePort() {
    return remotePort;
  }

  private class TunnelingMetastoreClientInvocationHandler implements InvocationHandler {

    private final TunnelConnectionManager tunnelConnectionManager;
    private final CloseableThriftHiveMetastoreIface client;

    private TunnelingMetastoreClientInvocationHandler(
        TunnelConnectionManager tunnelConnectionManager,
        CloseableThriftHiveMetastoreIface client) {
      this.tunnelConnectionManager = tunnelConnectionManager;
      this.client = client;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      switch (method.getName()) {
      case "close":
        method.invoke(client, args);
        tunnelConnectionManager.close();
        return null;
      case "open":
      case "reconnect":
        tunnelConnectionManager.ensureOpen();
        return method.invoke(client, args);
      default:
        return method.invoke(client, args);
      }
    }

    private CloseableThriftHiveMetastoreIface newInstance() {
      TunnelingMetastoreClientInvocationHandler tunneledHandler = new TunnelingMetastoreClientInvocationHandler(
          tunnelConnectionManager, client);
      return (CloseableThriftHiveMetastoreIface) Proxy.newProxyInstance(getClass().getClassLoader(), INTERFACES,
          tunneledHandler);
    }
  }
}
