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

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;
import com.hotels.hcommon.hive.metastore.conf.HiveConfFactory;
import com.hotels.hcommon.hive.metastore.util.MetaStoreUriNormaliser;

public class CloseableThriftHiveMetastoreIfaceClientFactory {

  private final MetaStoreClientFactory metaStoreClientFactory;

  public CloseableThriftHiveMetastoreIfaceClientFactory(MetaStoreClientFactory metaStoreClientFactory) {
    this.metaStoreClientFactory = metaStoreClientFactory;
  }

  public CloseableThriftHiveMetastoreIface newInstance(AbstractMetaStore metaStore) {
    Map<String, String> properties = new HashMap<>();
    String uris = MetaStoreUriNormaliser.normaliseMetaStoreUris(metaStore.getRemoteMetaStoreUris());
    properties.put(ConfVars.METASTOREURIS.varname, uris);
    HiveConfFactory confFactory = new HiveConfFactory(Collections.<String> emptyList(), properties);

    String name = metaStore.getName().toLowerCase();
    if (metaStore.getConnectionType() == TUNNELED) {

      MetastoreTunnel metastoreTunnel = metaStore.getMetastoreTunnel();

      // properties.put(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname, metastoreTunnel.getLocalhost());
      // properties.put(WaggleDanceHiveConfVars.SSH_PORT.varname, String.valueOf(metastoreTunnel.getPort()));
      // properties.put(WaggleDanceHiveConfVars.SSH_ROUTE.varname, metastoreTunnel.getRoute());
      // properties.put(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname, metastoreTunnel.getKnownHosts());
      // properties.put(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, metastoreTunnel.getPrivateKeys());
      // properties.put(WaggleDanceHiveConfVars.SSH_SESSION_TIMEOUT.varname,
      // String.valueOf(metastoreTunnel.getTimeout()));
      // properties
      // .put(WaggleDanceHiveConfVars.SSH_STRICT_HOST_KEY_CHECKING.varname,
      // metastoreTunnel.getStrictHostKeyChecking());

      return metaStoreClientFactory.newInstance(confFactory.newInstance(), "waggledance-" + name, 3, metastoreTunnel);
    } else {
      return metaStoreClientFactory.newInstance(confFactory.newInstance(), "waggledance-" + name, 3);
    }
  }
}
