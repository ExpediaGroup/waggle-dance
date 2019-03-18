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
package com.hotels.bdp.waggledance.client.tunnelling;

import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.waggledance.client.DefaultMetaStoreClientFactory;
import com.hotels.bdp.waggledance.client.MetaStoreClientFactory;

public class HiveMetaStoreClientSupplierFactory {

  private final MetaStoreClientFactory factory;

  @VisibleForTesting
  HiveMetaStoreClientSupplierFactory(MetaStoreClientFactory factory) {
    this.factory = factory;
  }

  HiveMetaStoreClientSupplierFactory() {
    this(new DefaultMetaStoreClientFactory());
  }

  HiveMetaStoreClientSupplier newInstance(HiveConf hiveConf, String name, int reconnectionRetries, int connectionTimeout) {
    return new HiveMetaStoreClientSupplier(factory, hiveConf, name, reconnectionRetries, connectionTimeout);
  }
}
