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

import static org.springframework.util.StringUtils.isEmpty;

import static com.hotels.bdp.waggledance.client.TunnelConnectionManagerFactory.FIRST_AVAILABLE_PORT;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pastdev.jsch.tunnel.TunnelConnectionManager;

public class TunnelingMetaStoreClientFactory extends MetaStoreClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(TunnelingMetaStoreClientFactory.class);


  private final SessionFactorySupplierFactory sessionFactorySupplierFactory;

  private TunnelConnectionManagerFactory tunnelConnectionManagerFactory;
  private HiveConf hiveConf;
  private String remoteHost;
  private Integer remotePort;
  private String sshRoute;
  private String localHost;
  private TunnelConnectionManager tunnelConnectionManager;

  public TunnelingMetaStoreClientFactory() {
    this.sessionFactorySupplierFactory = new SessionFactorySupplierFactory();
  }

  @Override
  public CloseableThriftHiveMetastoreIface newInstance(HiveConf conf, String name, int reconnectionRetries) {
    hiveConf = conf;
    if (isEmpty(hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname))) {
      return super.newInstance(hiveConf, name, reconnectionRetries);
    }
    tunnelConnectionManagerFactory = new TunnelConnectionManagerFactory(
        sessionFactorySupplierFactory.newInstance(hiveConf));
    URI metaStoreUri = URI.create(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
    remoteHost = metaStoreUri.getHost();
    remotePort = metaStoreUri.getPort();
    sshRoute = hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname);
    localHost = hiveConf.get(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname, "localhost");
    tunnelConnectionManager = createTunnelConnectionManager();
    TunnelHandler.openTunnel(tunnelConnectionManager, sshRoute, localHost, remoteHost, remotePort);
    HiveConf localHiveConf = createLocalHiveConf();
    LOG.info("Metastore URI {} is being proxied to {}", hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS),
        localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
    return createTunnelingMetastoreClient(localHiveConf, name, reconnectionRetries);
  }

  private TunnelConnectionManager createTunnelConnectionManager() {
    return tunnelConnectionManagerFactory.create(sshRoute, localHost, FIRST_AVAILABLE_PORT, remoteHost, remotePort);
  }

  private CloseableThriftHiveMetastoreIface createTunnelingMetastoreClient(
      HiveConf localHiveConf,
      String name,
      Integer reconnectionRetries) {
    TunnelingMetastoreClientInvocationHandler tunneledHandler = new TunnelingMetastoreClientInvocationHandler(
        tunnelConnectionManager, super.newInstance(localHiveConf, name, reconnectionRetries));
    return (CloseableThriftHiveMetastoreIface) Proxy.newProxyInstance(getClass().getClassLoader(), INTERFACES,
        tunneledHandler);
  }

  private HiveConf createLocalHiveConf() {
    int localPort = tunnelConnectionManager.getTunnel(remoteHost, remotePort).getAssignedLocalPort();
    String proxyMetaStoreUris = "thrift://" + localHost + ":" + localPort;
    HiveConf localHiveConf = new HiveConf(hiveConf);
    localHiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, proxyMetaStoreUris);
    return localHiveConf;
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
