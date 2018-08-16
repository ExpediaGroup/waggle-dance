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
package com.hotels.bdp.waggledance.client.tunneling;

import static org.springframework.util.StringUtils.isEmpty;

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
import com.hotels.bdp.waggledance.client.WaggleDanceHiveConfVars;
import com.hotels.hcommon.ssh.MethodChecker;
import com.hotels.hcommon.ssh.SshException;
import com.hotels.hcommon.ssh.TunnelableFactory;

public class TunnelingMetaStoreClientFactory implements MetaStoreClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TunnelingMetaStoreClientFactory.class);

  @VisibleForTesting
  final MethodChecker METHOD_CHECKER = new MetastoreClientMethodChecker();

  private final TunnelableFactorySupplier tunnelableFactorySupplier;
  private final MetaStoreClientFactory defaultFactory;
  private final LocalHiveConfFactory localHiveConfFactory;
  private final HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory;

  public TunnelingMetaStoreClientFactory() {
    this(new TunnelableFactorySupplier(), new DefaultMetaStoreClientFactory(), new LocalHiveConfFactory(),
        new HiveMetaStoreClientSupplierFactory());
  }

  @VisibleForTesting
  TunnelingMetaStoreClientFactory(
      TunnelableFactorySupplier tunnelableFactorySupplier,
      MetaStoreClientFactory defaultFactory,
      LocalHiveConfFactory localHiveConfFactory,
      HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory) {
    this.tunnelableFactorySupplier = tunnelableFactorySupplier;
    this.defaultFactory = defaultFactory;
    this.localHiveConfFactory = localHiveConfFactory;
    this.hiveMetaStoreClientSupplierFactory = hiveMetaStoreClientSupplierFactory;
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

    HiveConf localHiveConf = localHiveConfFactory.newInstance(localHost, localPort, hiveConf);

    TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory = tunnelableFactorySupplier
        .get(localHiveConf);

    LOG
        .info("Metastore URI {} is being proxied through {}", hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS),
            localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));

    HiveMetaStoreClientSupplier supplier = hiveMetaStoreClientSupplierFactory
        .newInstance(localHiveConf, name, reconnectionRetries);

    return (CloseableThriftHiveMetastoreIface) tunnelableFactory
        .wrap(supplier, METHOD_CHECKER, localHost, localPort, remoteHost, remotePort);
  }

  private int getLocalPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException | RuntimeException e) {
      throw new SshException("Unable to bind to a free localhost port", e);
    }
  }
}
