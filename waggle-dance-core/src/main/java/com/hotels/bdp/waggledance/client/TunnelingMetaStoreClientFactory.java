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

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.URI;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.hcommon.ssh.MethodChecker;
import com.hotels.hcommon.ssh.SshException;
import com.hotels.hcommon.ssh.TunnelableFactory;

public class TunnelingMetaStoreClientFactory extends MetaStoreClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TunnelingMetaStoreClientFactory.class);

  private static class MetastoreClientMethodChecker implements MethodChecker {
    @Override
    public boolean isTunnelled(Method method) {
      return "open".equals(method.getName()) || "reconnect".equals(method.getName());
    }

    @Override
    public boolean isShutdown(Method method) {
      return "close".equals(method.getName());
    }
  }

  @VisibleForTesting
  static final MethodChecker METHOD_CHECKER = new MetastoreClientMethodChecker();

  private static int getLocalPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException | RuntimeException e) {
      throw new SshException("Unable to bind to a free localhost port", e);
    }
  }

  private final TunnelableFactorySupplier tunnelableFactorySupplier;

  public TunnelingMetaStoreClientFactory() {
    this(new TunnelableFactorySupplier());
  }

  public TunnelingMetaStoreClientFactory(TunnelableFactorySupplier tunnelableFactorySupplier) {
    this.tunnelableFactorySupplier = tunnelableFactorySupplier;
  }

  @Override
  public CloseableThriftHiveMetastoreIface newInstance(HiveConf hiveConf, String name, int reconnectionRetries) {
    if (isEmpty(hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname))) {
      return super.newInstance(hiveConf, name, reconnectionRetries);
    }

    String localHost = hiveConf.get(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname);
    int localPort = getLocalPort();

    URI metaStoreUri = URI.create(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
    String remoteHost = metaStoreUri.getHost();
    int remotePort = metaStoreUri.getPort();

    TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory = tunnelableFactorySupplier.get(hiveConf);

    HiveConf localHiveConf = createLocalHiveConf(localHost, localPort, remoteHost, remotePort, hiveConf);
    LOG.info("Metastore URI {} is being proxied to {}", hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS),
        localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));

    return (CloseableThriftHiveMetastoreIface) tunnelableFactory.wrap(
        super.newInstance(localHiveConf, name, reconnectionRetries), METHOD_CHECKER, localHost, localPort, remoteHost,
        remotePort);
  }

  private HiveConf createLocalHiveConf(
      String localHost,
      int localPort,
      String remoteHost,
      int remotePort,
      HiveConf hiveConf) {
    String proxyMetaStoreUris = "thrift://" + localHost + ":" + localPort;
    HiveConf localHiveConf = new HiveConf(hiveConf);
    localHiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, proxyMetaStoreUris);
    return localHiveConf;
  }
}
