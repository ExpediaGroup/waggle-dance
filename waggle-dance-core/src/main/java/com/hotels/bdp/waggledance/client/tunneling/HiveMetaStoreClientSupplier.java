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
package com.hotels.bdp.waggledance.client.tunneling;

import org.apache.hadoop.hive.conf.HiveConf;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.client.MetaStoreClientFactory;
import com.hotels.hcommon.ssh.TunnelableSupplier;

class HiveMetaStoreClientSupplier implements TunnelableSupplier<CloseableThriftHiveMetastoreIface> {
  private final MetaStoreClientFactory factory;
  private final HiveConf hiveConf;
  private final String name;
  private final int reconnectionRetries;

  HiveMetaStoreClientSupplier(MetaStoreClientFactory factory, HiveConf hiveConf, String name, int reconnectionRetries) {
    this.factory = factory;
    this.hiveConf = hiveConf;
    this.name = name;
    this.reconnectionRetries = reconnectionRetries;
  }

  @Override
  public CloseableThriftHiveMetastoreIface get() {
    return factory.newInstance(hiveConf, name, reconnectionRetries);
  }

}
