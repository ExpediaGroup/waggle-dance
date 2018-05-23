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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.base.Joiner;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetastoreTunnel;
import com.hotels.hcommon.hive.metastore.client.HiveMetaStoreClientSupplier;
import com.hotels.hcommon.hive.metastore.client.reconnecting.ReconnectingMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.tunnelling.TunnellingMetaStoreClientSupplierBuilder;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.conf.HiveConfFactory;

public class ReconnectingTunnellingMetaStoreClientFactory implements MetaStoreClientFactory {

  private final AbstractMetaStore metaStore;

  public ReconnectingTunnellingMetaStoreClientFactory(AbstractMetaStore metaStore) {
    this.metaStore = metaStore;
  }

  @Override
  public CloseableMetaStoreClient newInstance() {
    Map<String, String> properties = new HashMap<>();
    String uris = normaliseMetaStoreUris(metaStore.getRemoteMetaStoreUris());
    MetastoreTunnel metastoreTunnel = metaStore.getMetastoreTunnel();
    properties.put(HiveConf.ConfVars.METASTOREURIS.varname, uris);
    HiveConfFactory confFactory = new HiveConfFactory(Collections.<String> emptyList(), properties);
    HiveConf hiveConf = confFactory.newInstance();
    MetaStoreClientFactory metaStoreClientFactory = new ReconnectingMetaStoreClientFactory(hiveConf,
        metaStore.getName().toLowerCase(), 10);

    if (metastoreTunnel != null) {
      return new TunnellingMetaStoreClientSupplierBuilder()
          .withRoute(metastoreTunnel.getRoute())
          .withKnownHosts(metastoreTunnel.getKnownHosts())
          .withLocalHost(metastoreTunnel.getLocalhost())
          .withPort(metastoreTunnel.getPort())
          .withPrivateKeys(metastoreTunnel.getPrivateKeys())
          .withTimeout(metastoreTunnel.getTimeout())
          .withStrictHostKeyChecking(metastoreTunnel.getStrictHostKeyChecking())
          .build(hiveConf, metaStoreClientFactory).get();
    } else {
      return new HiveMetaStoreClientSupplier(metaStoreClientFactory).get();
    }
  }

  private String normaliseMetaStoreUris(String metaStoreUris) {
    try {
      String[] rawUris = metaStoreUris.split(",");
      Set<String> uris = new TreeSet<>();
      for (String rawUri : rawUris) {
        URI uri = new URI(rawUri);
        uris.add(uri.toString());
      }
      return Joiner.on(",").join(uris);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }
}
