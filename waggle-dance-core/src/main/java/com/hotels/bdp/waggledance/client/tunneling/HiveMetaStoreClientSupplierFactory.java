package com.hotels.bdp.waggledance.client.tunneling;

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

  HiveMetaStoreClientSupplier newInstance(HiveConf hiveConf, String name, int reconnectionRetries) {
    return new HiveMetaStoreClientSupplier(factory, hiveConf, name, reconnectionRetries);
  }
}
