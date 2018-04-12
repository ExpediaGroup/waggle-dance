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

import static org.springframework.util.StringUtils.isEmpty;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import static com.hotels.bdp.waggledance.client.TunnelConnectionManagerFactory.FIRST_AVAILABLE_PORT;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

public class TunnelingMetaStoreClientFactory extends MetaStoreClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TunnelingMetaStoreClientFactory.class);

  private static class TunnelingMetastoreClientInvocationHandler implements InvocationHandler {
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

  private final TunnelHandler tunnelHandler;

  public TunnelingMetaStoreClientFactory() {
    this(new TunnelHandler());
  }

  public TunnelingMetaStoreClientFactory(TunnelHandler tunnelHandler) {
    this.tunnelHandler = tunnelHandler;
  }

  @Override
  public CloseableThriftHiveMetastoreIface newInstance(HiveConf hiveConf, String name, int reconnectionRetries) {
    if (isEmpty(hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname))) {
      return super.newInstance(hiveConf, name, reconnectionRetries);
    }
    TunnelConnectionManagerFactory tunnelConnectionManagerFactory = createTunnelConnectionManagerFactory(hiveConf);
    URI metaStoreUri = URI.create(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
    String remoteHost = metaStoreUri.getHost();
    int remotePort = metaStoreUri.getPort();
    String sshRoute = hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname);
    String localHost = hiveConf.get(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname, "localhost");
    TunnelConnectionManager tunnelConnectionManager = tunnelConnectionManagerFactory.create(sshRoute, localHost,
        FIRST_AVAILABLE_PORT, remoteHost, remotePort);

    tunnelHandler.openTunnel(tunnelConnectionManager, sshRoute, localHost, remoteHost, remotePort);
    HiveConf localHiveConf = createLocalHiveConf(tunnelConnectionManager, localHost, remoteHost, remotePort, hiveConf);
    LOG.info("Metastore URI {} is being proxied to {}", hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS),
        localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));

    TunnelingMetastoreClientInvocationHandler tunneledHandler = new TunnelingMetastoreClientInvocationHandler(
        tunnelConnectionManager, super.newInstance(localHiveConf, name, reconnectionRetries));
    return (CloseableThriftHiveMetastoreIface) Proxy.newProxyInstance(getClass().getClassLoader(), INTERFACES,
        tunneledHandler);
  }

  // the code in this method is problematic for testing so we spy it out in the test, ideally we'd refactor this
  // elsewhere to remove the need for that
  @VisibleForTesting
  TunnelConnectionManagerFactory createTunnelConnectionManagerFactory(HiveConf hiveConf) {
    int sshPort = hiveConf.getInt(WaggleDanceHiveConfVars.SSH_PORT.varname, 22);
    String knownHosts = hiveConf.get(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname);
    String privateKeys = hiveConf.get(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname);
    int sshTimeout = hiveConf.getInt(WaggleDanceHiveConfVars.SSH_SESSION_TIMEOUT.varname, 0);
    String strictHostKeyChecking = hiveConf.get(WaggleDanceHiveConfVars.SSH_STRICT_HOST_KEY_CHECKING.varname, "no");

    checkArgument(sshPort > 0 && sshPort <= 65536,
        WaggleDanceHiveConfVars.SSH_PORT.varname + " must be a number between 1 and 65536");
    checkArgument(!isNullOrEmpty(privateKeys), WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname + " cannot be null");

    SessionFactorySupplier sessionFactorySupplier = new SessionFactorySupplier(sshPort, knownHosts,
        Arrays.asList(privateKeys.split(",")), sshTimeout, strictHostKeyChecking);

    return new TunnelConnectionManagerFactory(sessionFactorySupplier);
  }

  private HiveConf createLocalHiveConf(
      TunnelConnectionManager tunnelConnectionManager,
      String localHost,
      String remoteHost,
      int remotePort,
      HiveConf hiveConf) {
    int localPort = tunnelConnectionManager.getTunnel(remoteHost, remotePort).getAssignedLocalPort();
    String proxyMetaStoreUris = "thrift://" + localHost + ":" + localPort;
    HiveConf localHiveConf = new HiveConf(hiveConf);
    localHiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, proxyMetaStoreUris);
    return localHiveConf;
  }
}
