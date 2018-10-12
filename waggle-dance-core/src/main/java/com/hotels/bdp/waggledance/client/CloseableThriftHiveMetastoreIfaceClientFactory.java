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

import static com.hotels.bdp.waggledance.api.model.ConnectionType.TUNNELED;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.client.tunnelling.TunnelingMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;
import com.hotels.hcommon.hive.metastore.conf.HiveConfFactory;
import com.hotels.hcommon.hive.metastore.util.MetaStoreUriNormaliser;
import com.hotels.hcommon.ssh.SshSettings;
import com.hotels.hcommon.ssh.TunnelableFactory;

public class CloseableThriftHiveMetastoreIfaceClientFactory {

  private MetaStoreClientFactory metaStoreClientFactory;
  private SshSettings sshSettings;
  private HiveConf hiveConf;

  public CloseableThriftHiveMetastoreIfaceClientFactory() {
    metaStoreClientFactory = new DefaultMetaStoreClientFactory();
  }

  public CloseableThriftHiveMetastoreIface newInstance(AbstractMetaStore metaStore) {
    Map<String, String> properties = new HashMap<>();
    String uris = MetaStoreUriNormaliser.normaliseMetaStoreUris(metaStore.getRemoteMetaStoreUris());
    properties.put(ConfVars.METASTOREURIS.varname, uris);
    HiveConfFactory confFactory = new HiveConfFactory(Collections.<String> emptyList(), properties);
    String name = metaStore.getName().toLowerCase();

    if (metaStore.getConnectionType() == TUNNELED) {
      MetastoreTunnel metastoreTunnel = metaStore.getMetastoreTunnel();

      sshSettings = buildSshSettings(metastoreTunnel);
      metaStoreClientFactory = new TunnelingMetaStoreClientFactory(new TunnelableFactory<>(sshSettings),
          metastoreTunnel.getLocalhost());
    }

    hiveConf = confFactory.newInstance();
    return metaStoreClientFactory.newInstance(hiveConf, "waggledance-" + name, 3);
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

  @VisibleForTesting
  SshSettings getSshSettings() {
    return sshSettings;
  }

  @VisibleForTesting
  MetaStoreClientFactory getMetaStoreClientFactory() {
    return metaStoreClientFactory;
  }

  @VisibleForTesting
  HiveConf getHiveConf() {
    return hiveConf;
  }

}
