package com.hotels.bdp.waggledance.metastore;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;

public interface ThriftHiveMetaStoreClientFactory {
  public CloseableMetaStoreClient newInstance(AbstractMetaStore metaStore);
}
