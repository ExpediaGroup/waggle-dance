package com.hotels.bdp.waggledance.client;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.api.MetaException;

import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;

public class GlueClientFactory {

  public AWSCatalogMetastoreClient newInstance(HiveConf conf, HiveMetaHookLoader hook) throws MetaException {
    return new AWSCatalogMetastoreClient(conf, hook);
  }

}
