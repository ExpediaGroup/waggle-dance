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
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Before;
import org.junit.Test;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.client.tunnelling.TunnelingMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;

public class MetastoreClientFactorySupplierTest {

  private final String thriftUri = "thrift://host:port";
  private final String name = "Test";
  private final MetastoreTunnel metastoreTunnel = createMetastoreTunnel();
  private final AbstractMetaStore federatedMetaStore = AbstractMetaStore.newFederatedInstance(name, thriftUri);
  private MetastoreClientFactorySupplier supplier;

  @Before
  public void setUp() {
    supplier = new MetastoreClientFactorySupplier(federatedMetaStore);
  }

  @Test
  public void getDefaultMetaStoreClientFactory() {
    assertThat(supplier.get(), instanceOf(DefaultMetaStoreClientFactory.class));
  }

  @Test
  public void getTunnelingMetastoreClientFactory() {
    federatedMetaStore.setMetastoreTunnel(metastoreTunnel);
    supplier = new MetastoreClientFactorySupplier(federatedMetaStore);
    assertThat(supplier.get(), instanceOf(TunnelingMetaStoreClientFactory.class));
  }

  @Test
  public void getMetaStoreUris() {
    assertThat(supplier.getMetaStoreUris(), is(thriftUri));
  }

  @Test
  public void getMetaStoreName() {
    assertThat(supplier.getMetaStoreName(), is(name.toLowerCase()));
  }

  private MetastoreTunnel createMetastoreTunnel() {
    MetastoreTunnel metastoreTunnel = new MetastoreTunnel();
    metastoreTunnel.setLocalhost("localhost");
    metastoreTunnel.setPort(222);
    metastoreTunnel.setRoute("a -> b");
    metastoreTunnel.setKnownHosts("knownHosts");
    metastoreTunnel.setPrivateKeys("privateKeys");
    metastoreTunnel.setTimeout(123);
    metastoreTunnel.setStrictHostKeyChecking("yes");
    return metastoreTunnel;
  }

}
