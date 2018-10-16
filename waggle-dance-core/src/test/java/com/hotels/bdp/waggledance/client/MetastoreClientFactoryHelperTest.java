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

import org.junit.Test;
import org.mockito.Mock;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.client.tunnelling.TunnelingMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;

public class MetastoreClientFactoryHelperTest {

  private static final String THRIFT_URI = "thrift://host:port";

  private @Mock AbstractMetaStore metastore;
  private final String name = "test";
  private final String localhost = "localhost";
  private final String route = "a -> b";
  private final String knownHosts = "knownHosts";
  private final String privateKeys = "privateKeys";
  private final int timeout = 123;
  private final int port = 222;
  private final MetastoreTunnel metastoreTunnel = createMetastoreTunnel();
  private final AbstractMetaStore federatedMetaStore = AbstractMetaStore.newFederatedInstance(name, THRIFT_URI);
  private MetastoreClientFactoryHelper helper;

  @Test
  public void getDefaultMetaStoreClientFactory() {
    helper = new MetastoreClientFactoryHelper(federatedMetaStore);
    assertThat(helper.get(), instanceOf(DefaultMetaStoreClientFactory.class));
  }

  @Test
  public void getTunnelingMetastoreClientFactoryWithStrictHostKeyChecking() {
    metastoreTunnel.setStrictHostKeyChecking("yes");
    federatedMetaStore.setMetastoreTunnel(metastoreTunnel);
    helper = new MetastoreClientFactoryHelper(federatedMetaStore);
    assertThat(helper.get(), instanceOf(TunnelingMetaStoreClientFactory.class));
  }

  @Test
  public void getTunnelingMetastoreClientFactoryNoStrictHostKeyChecking() {
    metastoreTunnel.setStrictHostKeyChecking("no");
    federatedMetaStore.setMetastoreTunnel(metastoreTunnel);
    helper = new MetastoreClientFactoryHelper(federatedMetaStore);
    assertThat(helper.get(), instanceOf(TunnelingMetaStoreClientFactory.class));
  }

  private MetastoreTunnel createMetastoreTunnel() {
    MetastoreTunnel metastoreTunnel = new MetastoreTunnel();
    metastoreTunnel.setLocalhost(localhost);
    metastoreTunnel.setPort(port);
    metastoreTunnel.setRoute(route);
    metastoreTunnel.setKnownHosts(knownHosts);
    metastoreTunnel.setPrivateKeys(privateKeys);
    metastoreTunnel.setTimeout(timeout);
    return metastoreTunnel;
  }

}
