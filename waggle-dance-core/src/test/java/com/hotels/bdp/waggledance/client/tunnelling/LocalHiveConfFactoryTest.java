/**
 * Copyright (C) 2016-2018 Expedia, Inc.
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
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

public class LocalHiveConfFactoryTest {

  @Test
  public void getCorrectHiveConf() {
    String localHost = "localHost";
    int localPort = 10;
    String expectedUri = "thrift://" + localHost + ":" + localPort;

    HiveConf hiveConf = new HiveConf();
    HiveConf conf = new LocalHiveConfFactory().newInstance(localHost, localPort, hiveConf);
    assertThat(conf.getVar(HiveConf.ConfVars.METASTOREURIS), is(expectedUri));
    assertThat(conf, not(sameInstance(hiveConf)));
  }

}
