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
package com.hotels.bdp.waggledance.client.tunnelling;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.client.DefaultMetaStoreClientFactory;
import com.hotels.bdp.waggledance.client.MetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;
import com.hotels.hcommon.ssh.MethodChecker;
import com.hotels.hcommon.ssh.SshException;
import com.hotels.hcommon.ssh.TunnelableFactory;

public class TunnelingMetaStoreClientFactory implements MetaStoreClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TunnelingMetaStoreClientFactory.class);

  @VisibleForTesting
  final MethodChecker METHOD_CHECKER = new MetastoreClientMethodChecker();

  private final TunnelableFactorySupplier tunnelableFactorySupplier;
  private final MetaStoreClientFactory defaultFactory;
  private final HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory;

  public TunnelingMetaStoreClientFactory() {
    this(new TunnelableFactorySupplier(), new DefaultMetaStoreClientFactory(),
        new HiveMetaStoreClientSupplierFactory());
  }

  @VisibleForTesting
  TunnelingMetaStoreClientFactory(
      TunnelableFactorySupplier tunnelableFactorySupplier,
      MetaStoreClientFactory defaultFactory,
      HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory) {
    this.tunnelableFactorySupplier = tunnelableFactorySupplier;
    this.defaultFactory = defaultFactory;
    this.hiveMetaStoreClientSupplierFactory = hiveMetaStoreClientSupplierFactory;
  }

  @Override
  public CloseableThriftHiveMetastoreIface newInstance(
      HiveConf hiveConf,
      String name,
      int reconnectionRetries,
      MetastoreTunnel metastoreTunnel) {
    // if (isEmpty(hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname))) {
    if (metastoreTunnel == null) {
      return defaultFactory.newInstance(hiveConf, name, reconnectionRetries, metastoreTunnel);
    }

    // String localHost = hiveConf.get(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname);
    String localHost = metastoreTunnel.getLocalhost();
    int localPort = getLocalPort();
    String metastoreUri = hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS);

    URI metaStoreUri = URI.create(metastoreUri);
    String remoteHost = metaStoreUri.getHost();
    int remotePort = metaStoreUri.getPort();

    HiveConf localHiveConf = newLocalConf(localHost, localPort, hiveConf);
    String localMetastoreUri = localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS);

    TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory = tunnelableFactorySupplier
        .get(metastoreTunnel);

    LOG.info("Metastore URI {} is being proxied through {}", metastoreUri, localMetastoreUri);

    HiveMetaStoreClientSupplier supplier = hiveMetaStoreClientSupplierFactory
        .newInstance(localHiveConf, name, reconnectionRetries);

    return (CloseableThriftHiveMetastoreIface) tunnelableFactory
        .wrap(supplier, METHOD_CHECKER, localHost, localPort, remoteHost, remotePort);
  }

  private HiveConf newLocalConf(String localHost, int localPort, HiveConf hiveConf) {
    HiveConf localHiveConf = new HiveConf(hiveConf);
    String proxyMetaStoreUris = "thrift://" + localHost + ":" + localPort;
    localHiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, proxyMetaStoreUris);
    return localHiveConf;
  }

  private int getLocalPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException | RuntimeException e) {
      throw new SshException("Unable to bind to a free localhost port", e);
    }
  }

  @Override
  public CloseableThriftHiveMetastoreIface newInstance(HiveConf hiveConf, String name, int reconnectionRetries) {
    throw new IllegalStateException("Should not use tunnelling if there is no metastore tunnel used");
  }
}
