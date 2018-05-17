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
import com.hotels.hcommon.ssh.TunnelableSupplier;

public class TunnelingMetaStoreClientFactory implements MetaStoreClientFactory {
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

  private static class HiveMetaStoreClientSupplier implements TunnelableSupplier<CloseableThriftHiveMetastoreIface> {
    private final MetaStoreClientFactory factory;
    private final HiveConf hiveConf;
    private final String name;
    private final int reconnectionRetries;

    private HiveMetaStoreClientSupplier(HiveConf hiveConf, String name, int reconnectionRetries) {
      factory = new DefaultMetaStoreClientFactory();
      this.hiveConf = hiveConf;
      this.name = name;
      this.reconnectionRetries = reconnectionRetries;
    }

    @Override
    public CloseableThriftHiveMetastoreIface get() {
      return factory.newInstance(hiveConf, name, reconnectionRetries);
    }

  }

  private final TunnelableFactorySupplier tunnelableFactorySupplier;
  private final MetaStoreClientFactory defaultFactory;

  public TunnelingMetaStoreClientFactory() {
    this(new TunnelableFactorySupplier(), new DefaultMetaStoreClientFactory());
  }

  @VisibleForTesting
  TunnelingMetaStoreClientFactory(
      TunnelableFactorySupplier tunnelableFactorySupplier,
      MetaStoreClientFactory defaultFactory) {
    this.tunnelableFactorySupplier = tunnelableFactorySupplier;
    this.defaultFactory = defaultFactory;
  }

  @Override
  public CloseableThriftHiveMetastoreIface newInstance(HiveConf hiveConf, String name, int reconnectionRetries) {
    if (isEmpty(hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname))) {
      return defaultFactory.newInstance(hiveConf, name, reconnectionRetries);
    }

    String localHost = hiveConf.get(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname);
    int localPort = getLocalPort();

    URI metaStoreUri = URI.create(hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
    String remoteHost = metaStoreUri.getHost();
    int remotePort = metaStoreUri.getPort();

    HiveConf localHiveConf = createLocalHiveConf(localHost, localPort, remoteHost, remotePort, hiveConf);

    TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory = tunnelableFactorySupplier
        .get(localHiveConf);

    LOG.info("Metastore URI {} is being proxied to {}", localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS),
        hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));

    HiveMetaStoreClientSupplier supplier = new HiveMetaStoreClientSupplier(localHiveConf, name, reconnectionRetries);
    return (CloseableThriftHiveMetastoreIface) tunnelableFactory.wrap(supplier, METHOD_CHECKER, localHost, localPort,
        remoteHost, remotePort);
  }

  private HiveConf createLocalHiveConf(
      String localHost,
      int localPort,
      String remoteHost,
      int remotePort,
      HiveConf hiveConf) {
    HiveConf localHiveConf = new HiveConf(hiveConf);
    String proxyMetaStoreUris = "thrift://" + localHost + ":" + localPort;
    localHiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, proxyMetaStoreUris);
    return localHiveConf;
  }

}
