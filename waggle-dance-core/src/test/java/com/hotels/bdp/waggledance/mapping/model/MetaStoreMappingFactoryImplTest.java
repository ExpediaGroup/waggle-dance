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
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;

import java.util.Arrays;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetastoreTunnel;
import com.hotels.bdp.waggledance.client.MetaStoreClientFactory;
import com.hotels.bdp.waggledance.client.WaggleDanceHiveConfVars;
import com.hotels.bdp.waggledance.mapping.service.PrefixNamingStrategy;
import com.hotels.bdp.waggledance.server.security.AccessControlHandlerFactory;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

@RunWith(MockitoJUnitRunner.class)
public class MetaStoreMappingFactoryImplTest {

  private static final String TEST_DB = "test_db";

  public final @Rule ThriftHiveMetaStoreJUnitRule thrift = new ThriftHiveMetaStoreJUnitRule(TEST_DB);

  private @Mock PrefixNamingStrategy prefixNamingStrategy;
  private @Mock MetaStoreClientFactory metaStoreClientFactory;
  private @Mock AccessControlHandlerFactory accessControlHandlerFactory;

  private MetaStoreMappingFactoryImpl factory;

  @Before
  public void init() {
    when(prefixNamingStrategy.apply(any(AbstractMetaStore.class))).thenAnswer(new Answer<String>() {
      @Override
      public String answer(InvocationOnMock invocation) throws Throwable {
        return invocation.getArgumentAt(0, AbstractMetaStore.class).getDatabasePrefix();
      }
    });
    factory = new MetaStoreMappingFactoryImpl(prefixNamingStrategy, accessControlHandlerFactory);
  }

  @Test
  public void typical() {
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
  public void hiveConf() throws Exception {
    ArgumentCaptor<HiveConf> hiveConfCaptor = ArgumentCaptor.forClass(HiveConf.class);

    factory = new MetaStoreMappingFactoryImpl(prefixNamingStrategy, metaStoreClientFactory,
        accessControlHandlerFactory);
    factory.newInstance(newFederatedInstance("fed1", thrift.getThriftConnectionUri()));
    verify(metaStoreClientFactory).newInstance(hiveConfCaptor.capture(), anyString(), anyInt());

    HiveConf hiveConf = hiveConfCaptor.getValue();
    assertThat(hiveConf.getVar(ConfVars.METASTOREURIS), is(thrift.getThriftConnectionUri()));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname), is(nullValue()));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_PORT.varname), is(nullValue()));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname), is(nullValue()));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname), is(nullValue()));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname), is(nullValue()));
  }

  @Test
  public void hiveConfForTunneling() throws Exception {
    ArgumentCaptor<HiveConf> hiveConfCaptor = ArgumentCaptor.forClass(HiveConf.class);

    factory = new MetaStoreMappingFactoryImpl(prefixNamingStrategy, metaStoreClientFactory,
        accessControlHandlerFactory);

    MetastoreTunnel metastoreTunnel = new MetastoreTunnel();
    metastoreTunnel.setLocalhost("local-machine");
    metastoreTunnel.setPort(2222);
    metastoreTunnel.setRoute("a -> b -> c");
    metastoreTunnel.setKnownHosts("knownHosts");
    metastoreTunnel.setPrivateKeys("privateKeys");
    AbstractMetaStore federatedMetaStore = newFederatedInstance("fed1", thrift.getThriftConnectionUri());
    federatedMetaStore.setMetastoreTunnel(metastoreTunnel);

    factory.newInstance(federatedMetaStore);
    verify(metaStoreClientFactory).newInstance(hiveConfCaptor.capture(), anyString(), anyInt());

    HiveConf hiveConf = hiveConfCaptor.getValue();
    assertThat(hiveConf.getVar(ConfVars.METASTOREURIS), is(thrift.getThriftConnectionUri()));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname), is("local-machine"));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_PORT.varname), is("2222"));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname), is("a -> b -> c"));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname), is("knownHosts"));
    assertThat(hiveConf.get(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname), is("privateKeys"));
  }

}
