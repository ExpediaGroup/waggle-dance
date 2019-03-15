/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import static org.mockito.Mockito.verify;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.client.MetaStoreClientFactory;
import com.hotels.bdp.waggledance.client.tunnelling.HiveMetaStoreClientSupplier;

@RunWith(MockitoJUnitRunner.class)
public class HiveMetaStoreClientSupplierTest {

  private @Mock HiveConf hiveConf;
  private @Mock MetaStoreClientFactory factory;

  @Test
  public void getMetaStoreClientFactoryInstance() {
    String name = "test";
    int reconnectionRetries = 10;
    int connectionTimeout = 10;
    HiveMetaStoreClientSupplier supplier = new HiveMetaStoreClientSupplier(factory, hiveConf, name,
        reconnectionRetries, connectionTimeout);
    supplier.get();
    verify(factory).newInstance(hiveConf, name, reconnectionRetries, connectionTimeout);
  }

}
