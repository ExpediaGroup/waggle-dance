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

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.ConnectionType;
import com.hotels.bdp.waggledance.client.tunnelling.TunnelingMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;
import com.hotels.hcommon.ssh.SshSettings;
import com.hotels.hcommon.ssh.TunnelableFactory;

public class MetastoreClientFactoryHelper {
  private final AbstractMetaStore metaStore;

  public MetastoreClientFactoryHelper(AbstractMetaStore metaStore) {
    this.metaStore = metaStore;
  }

  public MetaStoreClientFactory get() {
    MetaStoreClientFactory metaStoreClientFactory = new DefaultMetaStoreClientFactory();
    if (metaStore.getConnectionType() == ConnectionType.TUNNELED) {
      MetastoreTunnel metastoreTunnel = metaStore.getMetastoreTunnel();

      SshSettings sshSettings = buildSshSettings(metastoreTunnel);
      metaStoreClientFactory = new TunnelingMetaStoreClientFactory(new TunnelableFactory<>(sshSettings),
          metastoreTunnel.getLocalhost());
    }
    return metaStoreClientFactory;
  }

  private SshSettings buildSshSettings(MetastoreTunnel metastoreTunnel) {
    boolean strictHostKeyChecking = true;
    if (metastoreTunnel.getStrictHostKeyChecking().toLowerCase().equals("no")) {
      strictHostKeyChecking = false;
    }
    return SshSettings
        .builder()
        .withSshPort(metastoreTunnel.getPort())
        .withSessionTimeout(metastoreTunnel.getTimeout())
        .withRoute(metastoreTunnel.getRoute())
        .withKnownHosts(metastoreTunnel.getKnownHosts())
        .withLocalhost(metastoreTunnel.getLocalhost())
        .withPrivateKeys(metastoreTunnel.getPrivateKeys())
        .withStrictHostKeyChecking(strictHostKeyChecking)
        .build();
  }

}
