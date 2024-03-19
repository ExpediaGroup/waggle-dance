/**
 * Copyright (C) 2016-2024 Expedia, Inc.
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

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import lombok.AllArgsConstructor;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.client.tunnelling.TunnelingMetaStoreClientFactory;
import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import com.hotels.bdp.waggledance.context.CommonBeans;
import com.hotels.hcommon.hive.metastore.conf.HiveConfFactory;
import com.hotels.hcommon.hive.metastore.util.MetaStoreUriNormaliser;

@AllArgsConstructor
public class CloseableThriftHiveMetastoreIfaceClientFactory {

  private static final int DEFAULT_CLIENT_FACTORY_RECONNECTION_RETRY = 3;
  private final TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory;
  private final DefaultMetaStoreClientFactory defaultMetaStoreClientFactory;
  private final WaggleDanceConfiguration waggleDanceConfiguration;
  private final int defaultConnectionTimeout = (int) TimeUnit.SECONDS.toMillis(2L);

  public CloseableThriftHiveMetastoreIface newInstance(AbstractMetaStore metaStore) {
    Map<String, String> properties = new HashMap<>();
    if (waggleDanceConfiguration.getConfigurationProperties() != null) {
      properties.putAll(waggleDanceConfiguration.getConfigurationProperties());
    }
    return newHiveInstance(metaStore, properties);
  }

  private CloseableThriftHiveMetastoreIface newHiveInstance(
          AbstractMetaStore metaStore,
          Map<String, String> properties) {
    String uris = MetaStoreUriNormaliser.normaliseMetaStoreUris(metaStore.getRemoteMetaStoreUris());
    String name = metaStore.getName().toLowerCase(Locale.ROOT);

    // Connection timeout should not be less than 1
    // A timeout of zero is interpreted as an infinite timeout, so this is avoided
    int connectionTimeout = Math.max(1, defaultConnectionTimeout + (int) metaStore.getLatency());

    if (metaStore.getConnectionType() == TUNNELED) {
      return tunnelingMetaStoreClientFactory
          .newInstance(uris, metaStore.getMetastoreTunnel(), name, DEFAULT_CLIENT_FACTORY_RECONNECTION_RETRY,
              connectionTimeout, waggleDanceConfiguration.getConfigurationProperties());
    }
    properties.put(ConfVars.METASTOREURIS.varname, uris);
    properties.put(CommonBeans.IMPERSONATION_ENABLED_KEY,
        String.valueOf(metaStore.isImpersonationEnabled()));
    HiveConfFactory confFactory = new HiveConfFactory(Collections.emptyList(), properties);
    return defaultMetaStoreClientFactory
        .newInstance(confFactory.newInstance(), "waggledance-" + name, DEFAULT_CLIENT_FACTORY_RECONNECTION_RETRY,
            connectionTimeout);
  }
}
