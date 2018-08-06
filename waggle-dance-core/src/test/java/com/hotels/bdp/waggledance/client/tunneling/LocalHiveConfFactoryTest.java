package com.hotels.bdp.waggledance.client.tunneling;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

@RunWith(MockitoJUnitRunner.class)
public class LocalHiveConfFactoryTest {
  public @Rule ThriftHiveMetaStoreJUnitRule metastore = new ThriftHiveMetaStoreJUnitRule();

  private HiveConf hiveConf;

  @Test
  public void getCorrectHiveConf() {
    String localHost = "localHost";
    int localPort = 10;
    String expectedUri = "thrift://" + localHost + ":" + localPort;

    hiveConf = metastore.conf();
    HiveConf conf = new LocalHiveConfFactory().createLocalHiveConf(localHost, localPort, hiveConf);
    assertThat(conf.getVar(HiveConf.ConfVars.METASTOREURIS), is(expectedUri));
    assertFalse(conf.equals(hiveConf));
  }

}
