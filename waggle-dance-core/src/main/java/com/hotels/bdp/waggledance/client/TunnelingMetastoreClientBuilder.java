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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.JSchException;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

public class TunnelingMetastoreClientBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(TunnelingMetastoreClientBuilder.class);

  private HiveConf localHiveConf;
  private String name;
  private Integer reconnectionRetries;
  private TunnelConnectionManagerFactory tunnelConnectionManagerFactory;
  private String remoteHost;
  private String sshRoute;
  private String localHost;
  private Integer remotePort;
  private TunnelConnectionManager tunnelConnectionManager;

  public CloseableThriftHiveMetastoreIface build() {
    tunnelConnectionManager = tunnelConnectionManagerFactory.create(sshRoute, localHost,
        FIRST_AVAILABLE_PORT, remoteHost, remotePort);
    openTunnel();
    return clientFromLocalHiveConf(tunnelConnectionManager, localHiveConf);
  }

  private CloseableThriftHiveMetastoreIface clientFromLocalHiveConf(
      TunnelConnectionManager tunnelConnectionManager,
      HiveConf localHiveConf) {
    CloseableThriftHiveMetastoreIface client = new MetaStoreClientFactory().newInstance(localHiveConf, name,
        reconnectionRetries);
    TunnelingMetastoreClientInvocationHandler tunneledHandler = new TunnelingMetastoreClientInvocationHandler(
        tunnelConnectionManager, client);
    return (CloseableThriftHiveMetastoreIface) Proxy.newProxyInstance(getClass().getClassLoader(), INTERFACES,
        tunneledHandler);
  }

  private void openTunnel() {
    try {
      LOG.debug("Creating tunnel: {}:? -> {} -> {}:{}", localHost, sshRoute, remoteHost, remotePort);
      int localPort = tunnelConnectionManager.getTunnel(remoteHost, remotePort).getAssignedLocalPort();
      tunnelConnectionManager.open();
      LOG.debug("Tunnel created: {}:{} -> {} -> {}:{}", localHost, localPort, sshRoute, remoteHost, remotePort);

      localPort = tunnelConnectionManager.getTunnel(remoteHost, remotePort).getAssignedLocalPort();
      String proxyMetaStoreUris = "thrift://" + localHost + ":" + localPort;
      localHiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, proxyMetaStoreUris);
      LOG.info("Metastore URI {} is being proxied to {}", this.localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS),
          localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
    } catch (JSchException | RuntimeException e) {
      String message = String.format("Unable to establish SSH tunnel: '%s:?' -> '%s' -> '%s:%s'", localHost, sshRoute,
          remoteHost, remotePort);
      throw new WaggleDanceException(message, e);
    }
  }

  public TunnelingMetastoreClientBuilder setHiveConf(HiveConf hiveConf) {
    this.localHiveConf = new HiveConf(hiveConf);
    return this;
  }

  public TunnelingMetastoreClientBuilder setName(String name) {
    this.name = name;
    return this;
  }

  public TunnelingMetastoreClientBuilder setReconnectionRetries(Integer reconnectionRetries) {
    this.reconnectionRetries = reconnectionRetries;
    return this;
  }

  public TunnelingMetastoreClientBuilder setTunnelConnectionManagerFactory(
      TunnelConnectionManagerFactory tunnelConnectionManagerFactory) {
    this.tunnelConnectionManagerFactory = tunnelConnectionManagerFactory;
    return this;
  }

  public TunnelingMetastoreClientBuilder setRemoteHost(String remoteHost) {
    this.remoteHost = remoteHost;
    return this;
  }

  public TunnelingMetastoreClientBuilder setSSHRoute(String sshRoute) {
    this.sshRoute = sshRoute;
    return this;
  }

  public TunnelingMetastoreClientBuilder setLocalHost(String localHost) {
    this.localHost = localHost;
    return this;
  }

  public TunnelingMetastoreClientBuilder setRemotePort(Integer remotePort) {
    this.remotePort = remotePort;
    return this;
  }

  public HiveConf getHiveConf() {
    return localHiveConf;
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
  }
}
