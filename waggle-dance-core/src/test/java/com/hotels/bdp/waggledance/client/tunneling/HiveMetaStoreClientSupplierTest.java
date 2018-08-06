package com.hotels.bdp.waggledance.client.tunneling;

import static org.mockito.Mockito.verify;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.client.MetaStoreClientFactory;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

@RunWith(MockitoJUnitRunner.class)
public class HiveMetaStoreClientSupplierTest {

  public @Rule ThriftHiveMetaStoreJUnitRule metastore = new ThriftHiveMetaStoreJUnitRule();

  private @Mock MetaStoreClientFactory factory;
  private HiveConf hiveConf;

  @Test
  public void getMetaStoreClientFactoryInstance() {
    String name = "test";
    int reconnectionRetries = 10;
    hiveConf = metastore.conf();
    HiveMetaStoreClientSupplier supplier = new HiveMetaStoreClientSupplier(factory, hiveConf, name,
        reconnectionRetries);
    supplier.get();
    verify(factory).newInstance(hiveConf, name, reconnectionRetries);

  }

}
