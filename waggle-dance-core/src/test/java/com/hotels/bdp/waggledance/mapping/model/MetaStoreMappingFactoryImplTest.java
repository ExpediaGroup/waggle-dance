/**
 * Copyright (C) 2016-2024 Expedia, Inc.
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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;

import java.util.Arrays;

import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.DatabaseResolution;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIfaceClientFactory;
import com.hotels.bdp.waggledance.client.DefaultMetaStoreClientFactory;
import com.hotels.bdp.waggledance.client.GlueClientFactory;
import com.hotels.bdp.waggledance.client.SplitTrafficMetastoreClientFactory;
import com.hotels.bdp.waggledance.client.tunnelling.TunnelingMetaStoreClientFactory;
import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import com.hotels.bdp.waggledance.mapping.service.PrefixNamingStrategy;
import com.hotels.bdp.waggledance.server.security.AccessControlHandlerFactory;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

@RunWith(MockitoJUnitRunner.class)
public class MetaStoreMappingFactoryImplTest {

  private static final String TEST_DB = "test_db";

  public final @Rule ThriftHiveMetaStoreJUnitRule thrift = new ThriftHiveMetaStoreJUnitRule(TEST_DB);

  private @Mock WaggleDanceConfiguration waggleDanceConfiguration;
  private @Mock PrefixNamingStrategy prefixNamingStrategy;
  private @Mock AccessControlHandlerFactory accessControlHandlerFactory;
  private final CloseableThriftHiveMetastoreIfaceClientFactory metaStoreClientFactory = new CloseableThriftHiveMetastoreIfaceClientFactory(
      new TunnelingMetaStoreClientFactory(), new DefaultMetaStoreClientFactory(), new GlueClientFactory(),
      new WaggleDanceConfiguration(), new SplitTrafficMetastoreClientFactory());

  private MetaStoreMappingFactoryImpl factory;

  @Before
  public void init() {
    when(prefixNamingStrategy.apply(any(AbstractMetaStore.class)))
        .thenAnswer((Answer<String>) invocation -> ((AbstractMetaStore) invocation.getArgument(0)).getDatabasePrefix());
    factory = new MetaStoreMappingFactoryImpl(waggleDanceConfiguration, prefixNamingStrategy, metaStoreClientFactory,
        accessControlHandlerFactory);
  }

  @Test
  public void typicalPrefixed() {
    when(waggleDanceConfiguration.getDatabaseResolution()).thenReturn(DatabaseResolution.PREFIXED);
    AbstractMetaStore federatedMetaStore = newFederatedInstance("fed1", thrift.getThriftConnectionUri());
    MetaStoreMapping mapping = factory.newInstance(federatedMetaStore);
    assertThat(mapping, is(notNullValue()));
    verify(prefixNamingStrategy).apply(federatedMetaStore);
    verify(accessControlHandlerFactory).newInstance(federatedMetaStore);
    assertThat(mapping.getDatabasePrefix(), is("fed1_"));
    assertThat(mapping.getMetastoreMappingName(), is("fed1"));
  }

  @Test
  public void typicalNonPrefixed() {
    AbstractMetaStore federatedMetaStore = newFederatedInstance("fed1", thrift.getThriftConnectionUri());
    MetaStoreMapping mapping = factory.newInstance(federatedMetaStore);
    assertThat(mapping, is(notNullValue()));
    verify(prefixNamingStrategy).apply(federatedMetaStore);
    verify(accessControlHandlerFactory).newInstance(federatedMetaStore);
    assertThat(mapping.getDatabasePrefix(), is("fed1_"));
    assertThat(mapping.getMetastoreMappingName(), is("fed1"));
  }

  @Test
  public void reconnection() throws Exception {
    MetaStoreMapping mapping = factory.newInstance(newFederatedInstance("fed1", thrift.getThriftConnectionUri()));
    assertThat(mapping.getClient().get_all_databases(), is(Arrays.asList("default", "test_db")));
    mapping.close();
    assertThat(mapping.getClient().get_all_databases(), is(Arrays.asList("default", "test_db")));
  }

  @Test
  public void connectionLost() throws Exception {
    MetaStoreMapping mapping = factory.newInstance(newFederatedInstance("fed1", thrift.getThriftConnectionUri()));
    assertThat(mapping.getClient().get_all_databases(), is(Arrays.asList("default", "test_db")));
    // simulate disconnection
    thrift.client().reconnect();
    assertThat(mapping.getClient().get_all_databases(), is(Arrays.asList("default", "test_db")));
  }

  @Test
  public void unreachableMetastoreClient() {
    CloseableThriftHiveMetastoreIfaceClientFactory closeableThriftHiveMetastoreIfaceClientFactory = Mockito
        .mock(CloseableThriftHiveMetastoreIfaceClientFactory.class);
    MetaStoreMappingFactoryImpl factory = new MetaStoreMappingFactoryImpl(waggleDanceConfiguration,
        prefixNamingStrategy, closeableThriftHiveMetastoreIfaceClientFactory, accessControlHandlerFactory);
    AbstractMetaStore federatedMetaStore = newFederatedInstance("fed1", thrift.getThriftConnectionUri());
    when(closeableThriftHiveMetastoreIfaceClientFactory.newInstance(federatedMetaStore))
        .thenThrow(new RuntimeException("Cannot create client"));

    MetaStoreMapping mapping = factory.newInstance(federatedMetaStore);
    assertThat(mapping, is(notNullValue()));
    assertThat(mapping.isAvailable(), is(false));
    try {
      mapping.getClient().getStatusDetails();
    } catch (TException e) {
      assertThat("Metastore 'fed1' unavailable", is(e.getMessage()));
    }
  }

  @Test
  public void loadMetastoreFilterHookFromConfig() {
    AbstractMetaStore federatedMetaStore = newFederatedInstance("fed1", thrift.getThriftConnectionUri());
    federatedMetaStore.setHiveMetastoreFilterHook(PrefixingMetastoreFilter.class.getName());
    MetaStoreMapping mapping = factory.newInstance(federatedMetaStore);
    assertThat(mapping, is(notNullValue()));
    assertThat(mapping.getMetastoreFilter(), instanceOf(PrefixingMetastoreFilter.class));
  }

  @Test
  public void loadDefaultMetastoreFilterHook() {
    AbstractMetaStore federatedMetaStore = newFederatedInstance("fed1", thrift.getThriftConnectionUri());
    MetaStoreMapping mapping = factory.newInstance(federatedMetaStore);
    assertThat(mapping, is(notNullValue()));
    assertThat(mapping.getMetastoreFilter(), instanceOf(DefaultMetaStoreFilterHookImpl.class));
  }
}
