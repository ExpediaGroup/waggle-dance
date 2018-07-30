package com.hotels.bdp.waggledance.client.tunneling;

import org.apache.hadoop.hive.conf.HiveConf;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.client.DefaultMetaStoreClientFactory;
import com.hotels.bdp.waggledance.client.MetaStoreClientFactory;
import com.hotels.hcommon.ssh.TunnelableSupplier;

public class HiveMetaStoreClientSupplier implements TunnelableSupplier<CloseableThriftHiveMetastoreIface> {
  private final MetaStoreClientFactory factory;
  private final HiveConf hiveConf;
  private final String name;
  private final int reconnectionRetries;

  public HiveMetaStoreClientSupplier(HiveConf hiveConf, String name, int reconnectionRetries) {
    factory = new DefaultMetaStoreClientFactory();
    this.hiveConf = hiveConf;
    this.name = name;
    this.reconnectionRetries = reconnectionRetries;
  }

  @Override
  public CloseableThriftHiveMetastoreIface get() {
    return factory.newInstance(hiveConf, name, reconnectionRetries);
  }

}
