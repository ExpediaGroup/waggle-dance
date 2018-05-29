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

package com.hotels.bdp.waggledance.metastore;

import org.apache.hadoop.hive.conf.HiveConf;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetastoreTunnel;
import com.hotels.hcommon.hive.metastore.client.HiveMetaStoreClientSupplier;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.tunnelling.TunnellingMetaStoreClientSupplierBuilder;

class ReconnectingTunnellingMetaStoreClientFactory implements MetaStoreClientFactory {

  private final AbstractMetaStore metaStore;

  ReconnectingTunnellingMetaStoreClientFactory(AbstractMetaStore metaStore) {
    this.metaStore = metaStore;
  }

  @Override
  public CloseableMetaStoreClient newInstance(HiveConf hiveConf, String name) {
    MetastoreTunnel metastoreTunnel = metaStore.getMetastoreTunnel();
    MetaStoreClientFactory metaStoreClientFactory = new ReconnectingMetaStoreClientFactory(10);

    if (metastoreTunnel != null) {
      return new TunnellingMetaStoreClientSupplierBuilder()
          .withName(name)
          .withRoute(metastoreTunnel.getRoute())
          .withKnownHosts(metastoreTunnel.getKnownHosts())
          .withLocalHost(metastoreTunnel.getLocalhost())
          .withPort(metastoreTunnel.getPort())
          .withPrivateKeys(metastoreTunnel.getPrivateKeys())
          .withTimeout(metastoreTunnel.getTimeout())
          .withStrictHostKeyChecking(metastoreTunnel.getStrictHostKeyChecking())
          .build(hiveConf, metaStoreClientFactory).get();
    } else {
      return new HiveMetaStoreClientSupplier(metaStoreClientFactory, hiveConf, name).get();
    }
  }
}
