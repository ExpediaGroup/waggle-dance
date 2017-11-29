/**
 * Copyright (C) 2016-2017 Expedia Inc.
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
package com.hotels.bdp.waggledance.mapping.model;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.client.pool.MetaStoreClientPool;
import com.hotels.bdp.waggledance.mapping.service.PrefixNamingStrategy;
import com.hotels.bdp.waggledance.server.security.AccessControlHandlerFactory;

@RunWith(MockitoJUnitRunner.class)
public class MetaStoreMappingFactoryImplTest {

  private @Mock PrefixNamingStrategy prefixNamingStrategy;
  private @Mock AccessControlHandlerFactory accessControlHandlerFactory;
  private @Mock MetaStoreClientPool metaStoreClientPool;
  private @Mock CloseableThriftHiveMetastoreIface client;

  private MetaStoreMappingFactoryImpl factory;

  @Before
  public void init() {
    when(prefixNamingStrategy.apply(any(AbstractMetaStore.class))).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        return invocation.getArgumentAt(0, AbstractMetaStore.class).getDatabasePrefix();
      }
    });
    factory = new MetaStoreMappingFactoryImpl(prefixNamingStrategy, metaStoreClientPool, accessControlHandlerFactory);
  }

  @Test
  public void typical() throws Exception {
    AbstractMetaStore federatedMetaStore = newFederatedInstance("fed1", "thrift://url:port");
    when(metaStoreClientPool.borrowObjectUnchecked(federatedMetaStore)).thenReturn(client);
    MetaStoreMapping mapping = factory.newInstance(federatedMetaStore);
    assertThat(mapping, is(notNullValue()));
    verify(accessControlHandlerFactory).newInstance(federatedMetaStore);
    assertThat(mapping.getDatabasePrefix(), is("fed1_"));
    assertThat(mapping.getMetastoreMappingName(), is("fed1"));
    assertThat(mapping.getClient(), is((Iface) client));
  }
}
