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

import java.net.URI;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.springframework.beans.factory.annotation.Autowired;

public class TunnelingMetaStoreClientFactory extends MetaStoreClientFactory {

  private final SessionFactorySupplierFactory sessionFactorySupplierFactory = new SessionFactorySupplierFactory();

  private @Autowired TunnelingMetastoreClientBuilder tunnelingMetastoreClientBuilder;

  @Override
  public CloseableThriftHiveMetastoreIface newInstance(HiveConf hiveConf, String name, int reconnectionRetries) {

    if (hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname) != null) {
      TunnelConnectionManagerFactory tunnelConnectionManagerFactory = new TunnelConnectionManagerFactory(
          sessionFactorySupplierFactory.newInstance(hiveConf));
      URI metaStoreUri = URI.create(hiveConf.getVar(ConfVars.METASTOREURIS));
      String remoteHost = metaStoreUri.getHost();
      Integer remotePort = metaStoreUri.getPort();
      String sshRoute = hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname);
      String localHost = hiveConf.get(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname, "localhost");
      tunnelingMetastoreClientBuilder
          .withHiveConf(hiveConf)
          .withName(name)
          .withReconnectionRetries(reconnectionRetries)
          .withTunnelConnectionManagerFactory(tunnelConnectionManagerFactory)
          .withRemoteHost(remoteHost)
          .withSSHRoute(sshRoute)
          .withLocalHost(localHost)
          .withRemotePort(remotePort)
          .build();
    }
    return super.newInstance(hiveConf, name, reconnectionRetries);
  }
}
