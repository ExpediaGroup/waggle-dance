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

import static com.hotels.bdp.waggledance.client.TunnelConnectionManagerFactory.FIRST_AVAILABLE_PORT;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.JSchException;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

public class TunnelingMetaStoreClientFactory extends MetaStoreClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TunnelingMetaStoreClientFactory.class);

  private static class TunnelingMetastoreClientInvocationHandler implements InvocationHandler {
    private final TunnelConnectionManager tunnelConnectionManager;
    private final CloseableThriftHiveMetastoreIface client;

    private TunnelingMetastoreClientInvocationHandler(TunnelConnectionManager tunnelConnectionManager,
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

  private final SessionFactorySupplierFactory sessionFactorySupplierFactory = new SessionFactorySupplierFactory();

  @Override
  public CloseableThriftHiveMetastoreIface newInstance(HiveConf hiveConf, String name, int reconnectionRetries) {
    if (hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname) != null) {
      TunnelConnectionManagerFactory tunnelConnectionManagerFactory = new TunnelConnectionManagerFactory(
          sessionFactorySupplierFactory.newInstance(hiveConf));
      return tunnel(hiveConf, tunnelConnectionManagerFactory, name, reconnectionRetries);
    }
    return super.newInstance(hiveConf, name, reconnectionRetries);
  }

  private CloseableThriftHiveMetastoreIface tunnel(HiveConf hiveConf,
                                                   TunnelConnectionManagerFactory tunnelConnectionManagerFactory,
                                                   String name, int reconnectionRetries) {
    URI metaStoreUri = URI.create(hiveConf.getVar(ConfVars.METASTOREURIS));
    String remoteHost = metaStoreUri.getHost();
    int remotePort = metaStoreUri.getPort();
    String sshRoute = hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname);
    String localHost = hiveConf.get(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname, "localhost");

    LOG.debug("Creating tunnel: {}:? -> {} -> {}:{}", localHost, sshRoute, remoteHost, remotePort);
    try {
      TunnelConnectionManager tunnelConnectionManager = tunnelConnectionManagerFactory.create(sshRoute, localHost,
          FIRST_AVAILABLE_PORT, remoteHost, remotePort);
      int localPort = tunnelConnectionManager.getTunnel(remoteHost, remotePort).getAssignedLocalPort();
      tunnelConnectionManager.open();
      LOG.debug("Tunnel created: {}:{} -> {} -> {}:{}", localHost, localPort, sshRoute, remoteHost, remotePort);

      localPort = tunnelConnectionManager.getTunnel(remoteHost, remotePort).getAssignedLocalPort();
      HiveConf localHiveConf = new HiveConf(hiveConf);
      String proxyMetaStoreUris = "thrift://" + localHost + ":" + localPort;
      localHiveConf.setVar(ConfVars.METASTOREURIS, proxyMetaStoreUris);
      LOG.info("Metastore URI {} is being proxied to {}", hiveConf.getVar(ConfVars.METASTOREURIS), proxyMetaStoreUris);

      CloseableThriftHiveMetastoreIface client = super.newInstance(localHiveConf, name, reconnectionRetries);

      TunnelingMetastoreClientInvocationHandler tunneledHandler = new TunnelingMetastoreClientInvocationHandler(
          tunnelConnectionManager, client);
      return (CloseableThriftHiveMetastoreIface) Proxy.newProxyInstance(getClass().getClassLoader(), INTERFACES,
          tunneledHandler);
    } catch (JSchException | RuntimeException e) {
      String message = String.format("Unable to establish SSH tunnel: '%s:?' -> '%s' -> '%s:%s'", localHost, sshRoute,
          remoteHost, remotePort);
      throw new WaggleDanceException(message, e);
    }
  }
}
