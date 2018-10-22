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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

import com.hotels.hcommon.hive.metastore.conf.HiveConfFactory;
import com.hotels.hcommon.hive.metastore.util.MetaStoreUriNormaliser;

public class CloseableThriftHiveMetastoreIfaceClientFactory {

  public CloseableThriftHiveMetastoreIface newInstance(MetastoreClientFactorySupplier supplier) {
    HiveConfFactory confFactory = createHiveConfFactory(supplier.getMetaStoreUris());
    HiveConf hiveConf = confFactory.newInstance();
    String name = supplier.getMetaStoreName();

    MetaStoreClientFactory metaStoreClientFactory = supplier.get();
    return metaStoreClientFactory.newInstance(hiveConf, "waggledance-" + name, 3);
  }

  private HiveConfFactory createHiveConfFactory(String remoteUris) {
    Map<String, String> properties = new HashMap<>();
    String uris = MetaStoreUriNormaliser.normaliseMetaStoreUris(remoteUris);
    properties.put(ConfVars.METASTOREURIS.varname, uris);
    return new HiveConfFactory(Collections.<String> emptyList(), properties);
  }

}
