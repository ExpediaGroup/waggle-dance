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
