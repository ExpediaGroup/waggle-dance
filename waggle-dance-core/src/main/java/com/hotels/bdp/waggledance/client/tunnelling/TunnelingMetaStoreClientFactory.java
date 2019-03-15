/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;
import com.hotels.hcommon.hive.metastore.conf.HiveConfFactory;
import com.hotels.hcommon.ssh.MethodChecker;
import com.hotels.hcommon.ssh.SshException;
import com.hotels.hcommon.ssh.TunnelableFactory;

public class TunnelingMetaStoreClientFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TunnelingMetaStoreClientFactory.class);

  @VisibleForTesting
  final MethodChecker METHOD_CHECKER = new MetastoreClientMethodChecker();

  private final TunnelableFactorySupplier tunnelableFactorySupplier;
  private final LocalHiveConfFactory localHiveConfFactory;
  private final HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory;

  public TunnelingMetaStoreClientFactory() {
    this(new TunnelableFactorySupplier(), new LocalHiveConfFactory(), new HiveMetaStoreClientSupplierFactory());
  }

  @VisibleForTesting
  TunnelingMetaStoreClientFactory(
      TunnelableFactorySupplier tunnelableFactorySupplier,
      LocalHiveConfFactory localHiveConfFactory,
      HiveMetaStoreClientSupplierFactory hiveMetaStoreClientSupplierFactory) {
    this.tunnelableFactorySupplier = tunnelableFactorySupplier;
    this.localHiveConfFactory = localHiveConfFactory;
    this.hiveMetaStoreClientSupplierFactory = hiveMetaStoreClientSupplierFactory;
  }

  public CloseableThriftHiveMetastoreIface newInstance(
      String uris,
      MetastoreTunnel metastoreTunnel,
      String name,
      int reconnectionRetries,
      int connectionTimeout) {
    String uri = uris;
    String[] urisSplit = uri.split(",");
    if (urisSplit.length > 1) {
      uri = urisSplit[0];
      LOG.debug("Can't support multiple uris '{}' for tunneling endpoint, using first '{}'", uris, uri);
    }
    String localHost = metastoreTunnel.getLocalhost();
    int localPort = getLocalPort();

    Map<String, String> properties = new HashMap<>();
    properties.put(ConfVars.METASTOREURIS.varname, uri);
    HiveConfFactory confFactory = new HiveConfFactory(Collections.<String>emptyList(), properties);
    HiveConf localHiveConf = localHiveConfFactory.newInstance(localHost, localPort, confFactory.newInstance());

    TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory = tunnelableFactorySupplier
        .get(metastoreTunnel);

    LOG
        .info("Metastore URI {} is being proxied through {}", uri,
            localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));

    HiveMetaStoreClientSupplier supplier = hiveMetaStoreClientSupplierFactory
        .newInstance(localHiveConf, name, reconnectionRetries, connectionTimeout);

    URI metaStoreUri = URI.create(uri);
    String remoteHost = metaStoreUri.getHost();
    int remotePort = metaStoreUri.getPort();
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
