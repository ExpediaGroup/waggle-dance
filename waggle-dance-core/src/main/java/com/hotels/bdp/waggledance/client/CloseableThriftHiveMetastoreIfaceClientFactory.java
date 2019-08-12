/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.client.tunnelling.TunnelingMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.conf.HiveConfFactory;
import com.hotels.hcommon.hive.metastore.util.MetaStoreUriNormaliser;

public class CloseableThriftHiveMetastoreIfaceClientFactory {

  private static final int DEFAULT_CLIENT_FACTORY_RECONNECTION_RETRY = 3;
  private final TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory;
  private final DefaultMetaStoreClientFactory defaultMetaStoreClientFactory;
  private final int defaultConnectionTimeout = (int) TimeUnit.SECONDS.toMillis(2L);
  private final WaggleDanceConfiguration waggleDanceConfiguration;

  public CloseableThriftHiveMetastoreIfaceClientFactory(
          TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory,
          DefaultMetaStoreClientFactory defaultMetaStoreClientFactory,
          WaggleDanceConfiguration waggleDanceConfiguration) {
    this.tunnelingMetaStoreClientFactory = tunnelingMetaStoreClientFactory;
    this.defaultMetaStoreClientFactory = defaultMetaStoreClientFactory;
    this.waggleDanceConfiguration = waggleDanceConfiguration;
  }

  public CloseableThriftHiveMetastoreIface newInstance(AbstractMetaStore metaStore) {
    String uris = MetaStoreUriNormaliser.normaliseMetaStoreUris(metaStore.getRemoteMetaStoreUris());
    String name = metaStore.getName().toLowerCase(Locale.ROOT);

    // Connection timeout should not be less than 1
    // A timeout of zero is interpreted as an infinite timeout, so this is avoided
    int connectionTimeout = Math.max(1, defaultConnectionTimeout + (int) metaStore.getLatency());

    if (metaStore.getConnectionType() == TUNNELED) {
      return tunnelingMetaStoreClientFactory
          .newInstance(uris, metaStore.getMetastoreTunnel(), name, DEFAULT_CLIENT_FACTORY_RECONNECTION_RETRY,
              connectionTimeout);
    }

    Map<String, String> properties = new HashMap<>();
    properties.put(ConfVars.METASTOREURIS.varname, uris);
    properties.putAll(waggleDanceConfiguration.getConfigurationProperties());

    HiveConfFactory confFactory = new HiveConfFactory(Collections.emptyList(), properties);
    return defaultMetaStoreClientFactory
        .newInstance(confFactory.newInstance(), "waggledance-" + name, DEFAULT_CLIENT_FACTORY_RECONNECTION_RETRY,
            connectionTimeout);
  }
}
