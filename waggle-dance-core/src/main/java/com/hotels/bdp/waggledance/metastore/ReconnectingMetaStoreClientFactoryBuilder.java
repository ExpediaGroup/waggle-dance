package com.hotels.bdp.waggledance.metastore;

import org.apache.hadoop.hive.conf.HiveConf;

import com.hotels.hcommon.hive.metastore.client.ReconnectingMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;

public class ReconnectingMetaStoreClientFactoryBuilder  {

  private HiveConf hiveConf;
  private int maxRetries;
  private String name;

  public ReconnectingMetaStoreClientFactoryBuilder withHiveConf(HiveConf hiveConf) {
    this.hiveConf = hiveConf;
    return this;
  }

  public ReconnectingMetaStoreClientFactoryBuilder withName(String name) {
    this.name = name;
    return this;
  }

  public ReconnectingMetaStoreClientFactoryBuilder withMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  public MetaStoreClientFactory build() {
    return new ReconnectingMetaStoreClientFactory(hiveConf, name, maxRetries);
  }

}
