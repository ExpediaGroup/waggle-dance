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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.util.MetaStoreUriNormaliser;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.conf.HiveConfFactory;

public class ThriftHiveMetaStoreClientFactory {

  public CloseableMetaStoreClient newInstance(AbstractMetaStore metaStore) {
    Map<String, String> properties = new HashMap<>();
    String uris = new MetaStoreUriNormaliser().normalise(metaStore.getRemoteMetaStoreUris());
    properties.put(HiveConf.ConfVars.METASTOREURIS.varname, uris);
    HiveConfFactory confFactory = new HiveConfFactory(Collections.<String> emptyList(), properties);
    HiveConf hiveConf = confFactory.newInstance();
    String name = metaStore.getName().toLowerCase();
    return new ReconnectingTunnellingMetaStoreClientFactory(metaStore).newInstance(hiveConf, name);
  }

}
