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

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;
import com.hotels.hcommon.ssh.SshSettings;
import com.hotels.hcommon.ssh.TunnelableFactory;

public class TunnelableFactorySupplier {

  /*
   * Note at the moment this class will create a factory each time the method is invoked. This means that each new
   * client will have its own exclusive SSH tunnel. We could cache the factory instead and use one tunnel for each
   * HiveConf, i.e. federated metastore configuration, and share the tunnel for all its clients. We must evaluate what's
   * more convenient/efficient in Waggle Dance.
   */
  public TunnelableFactory<CloseableThriftHiveMetastoreIface> get(MetastoreTunnel metastoreTunnel) {
    return new TunnelableFactory<>(buildSshSettings(metastoreTunnel));
  }

  @VisibleForTesting
  SshSettings buildSshSettings(MetastoreTunnel metastoreTunnel) {
    return SshSettings
        .builder()
        .withSshPort(metastoreTunnel.getPort())
        .withSessionTimeout(metastoreTunnel.getTimeout())
        .withRoute(metastoreTunnel.getRoute())
        .withKnownHosts(metastoreTunnel.getKnownHosts())
        .withLocalhost(metastoreTunnel.getLocalhost())
        .withPrivateKeys(metastoreTunnel.getPrivateKeys())
        .withStrictHostKeyChecking(metastoreTunnel.isStrictHostKeyCheckingEnabled())
        .build();
  }
}
