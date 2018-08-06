package com.hotels.bdp.waggledance.client.tunneling;

import org.apache.hadoop.hive.conf.HiveConf;

public class LocalHiveConfFactory {

  HiveConf createLocalHiveConf(String localHost, int localPort, HiveConf hiveConf) {
    HiveConf localHiveConf = new HiveConf(hiveConf);
    String proxyMetaStoreUris = "thrift://" + localHost + ":" + localPort;
    localHiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, proxyMetaStoreUris);
    return localHiveConf;
  }

}
