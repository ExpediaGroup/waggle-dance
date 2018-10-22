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
package com.hotels.bdp.waggledance.client;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.lang.reflect.Proxy;

import org.junit.Test;

import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;

public class CloseableThriftHiveMetastoreIfaceClientFactoryTest {

  @Test
  public void hiveConf() {
    FederatedMetaStore federatedMetaStore = new FederatedMetaStore("fed1", "thrift://localhost:1234");
    CloseableThriftHiveMetastoreIfaceClientFactory factory = new CloseableThriftHiveMetastoreIfaceClientFactory();
    MetastoreClientFactorySupplier helper = new MetastoreClientFactorySupplier(federatedMetaStore);
    CloseableThriftHiveMetastoreIface result = factory.newInstance(helper);
    assertThat(result, instanceOf(Proxy.class));
  }

}
