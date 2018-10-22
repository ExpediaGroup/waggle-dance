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
import com.hotels.bdp.waggledance.client.MetaStoreClientFactory;
import com.hotels.hcommon.ssh.MethodChecker;
import com.hotels.hcommon.ssh.SshException;
import com.hotels.hcommon.ssh.TunnelableFactory;

public class TunnelingMetaStoreClientFactory implements MetaStoreClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TunnelingMetaStoreClientFactory.class);

  @VisibleForTesting
  final MethodChecker METHOD_CHECKER = new MetastoreClientMethodChecker();

  private final TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory;
  private final String localhost;
  private final LocalHiveConfFactory localHiveConfFactory;
  private final HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory;

  public TunnelingMetaStoreClientFactory(
      TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory,
      String localhost) {
    this(new HiveMetaStoreClientSupplierFactory(), new LocalHiveConfFactory(), tunnelableFactory, localhost);
  }

  @VisibleForTesting
  TunnelingMetaStoreClientFactory(
      HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory,
      LocalHiveConfFactory localHiveConfFactory,
      TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory,
      String localhost) {
    this.hiveMetaStoreClientSupplierFactory = hiveMetaStoreClientSupplierFactory;
    this.localHiveConfFactory = localHiveConfFactory;
    this.tunnelableFactory = tunnelableFactory;
    this.localhost = localhost;

    if (localhost == null || tunnelableFactory == null) {
      throw new IllegalStateException(
          "A localhost and tunnelable factory need to be provided for TunnelingMetastoreClientFactory");
    }
  }

  @Override
  public CloseableThriftHiveMetastoreIface newInstance(HiveConf hiveConf, String name, int reconnectionRetries) {
    int localPort = getLocalPort();
    String remoteMetastoreUri = hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS);
    URI metaStoreUri = URI.create(remoteMetastoreUri);
    String remoteHost = metaStoreUri.getHost();
    int remotePort = metaStoreUri.getPort();

    HiveConf localHiveConf = localHiveConfFactory.newInstance(localhost, localPort, hiveConf);

    String localMetastoreUri = localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS);
    LOG.info("Metastore URI {} is being proxied through {}", remoteMetastoreUri, localMetastoreUri);

    HiveMetaStoreClientSupplier supplier = hiveMetaStoreClientSupplierFactory
        .newInstance(localHiveConf, name, reconnectionRetries);

    return (CloseableThriftHiveMetastoreIface) tunnelableFactory
        .wrap(supplier, METHOD_CHECKER, localhost, localPort, remoteHost, remotePort);
  }

  private int getLocalPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException | RuntimeException e) {
      throw new SshException("Unable to bind to a free localhost port", e);
    }
  }

}
