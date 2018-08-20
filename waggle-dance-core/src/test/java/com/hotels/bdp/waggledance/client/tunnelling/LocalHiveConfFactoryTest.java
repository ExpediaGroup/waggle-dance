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
package com.hotels.bdp.waggledance.client.tunnelling;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.client.tunnelling.LocalHiveConfFactory;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

@RunWith(MockitoJUnitRunner.class)
public class LocalHiveConfFactoryTest {

  public @Rule ThriftHiveMetaStoreJUnitRule metastore = new ThriftHiveMetaStoreJUnitRule();

  @Test
  public void getCorrectHiveConf() {
    String localHost = "localHost";
    int localPort = 10;
    String expectedUri = "thrift://" + localHost + ":" + localPort;

    HiveConf conf = new LocalHiveConfFactory().newInstance(localHost, localPort, metastore.conf());
    assertThat(conf.getVar(HiveConf.ConfVars.METASTOREURIS), is(expectedUri));
    assertFalse(conf.equals(metastore.conf()));
  }

}
