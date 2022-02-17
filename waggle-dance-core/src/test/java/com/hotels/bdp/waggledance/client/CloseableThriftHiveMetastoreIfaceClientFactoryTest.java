/**
 * Copyright (C) 2016-2022 Expedia, Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isA;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.GlueConfig;
import com.hotels.bdp.waggledance.client.adapter.MetastoreIfaceAdapter;
import com.hotels.bdp.waggledance.client.tunnelling.TunnelingMetaStoreClientFactory;
import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;

@RunWith(MockitoJUnitRunner.class)
public class CloseableThriftHiveMetastoreIfaceClientFactoryTest {

  private static final String THRIFT_URI = "thrift://host:port";

  private CloseableThriftHiveMetastoreIfaceClientFactory factory;
  private @Mock TunnelingMetaStoreClientFactory tunnelingMetaStoreClientFactory;
  private @Mock DefaultMetaStoreClientFactory defaultMetaStoreClientFactory;
  private @Mock GlueClientFactory glueClientFactory;
  private @Mock WaggleDanceConfiguration waggleDanceConfiguration;
  private final Map<String, String> configurationProperties = new HashMap<>();
  private @Mock AWSCatalogMetastoreClient glueClient;

  @Before
  public void setUp() {
    configurationProperties.put(ConfVars.METASTORETHRIFTCONNECTIONRETRIES.varname, "5");
    configurationProperties.put(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.varname, "6");
    configurationProperties.put(ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY.varname, "5");
    configurationProperties.put(ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT.varname, "true");
    configurationProperties.put(ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL.varname, "false");
    when(waggleDanceConfiguration.getConfigurationProperties()).thenReturn(configurationProperties);
    factory = new CloseableThriftHiveMetastoreIfaceClientFactory(tunnelingMetaStoreClientFactory,
        defaultMetaStoreClientFactory, glueClientFactory, waggleDanceConfiguration);
  }

  @Test
  public void defaultFactory() {
    ArgumentCaptor<HiveConf> hiveConfCaptor = ArgumentCaptor.forClass(HiveConf.class);

    factory.newInstance(newFederatedInstance("fed1", THRIFT_URI));
    verify(defaultMetaStoreClientFactory)
        .newInstance(hiveConfCaptor.capture(), eq("waggledance-fed1"), eq(3), eq(2000));
    verifyNoInteractions(tunnelingMetaStoreClientFactory);
    HiveConf hiveConf = hiveConfCaptor.getValue();
    assertThat(hiveConf.getVar(ConfVars.METASTOREURIS), is(THRIFT_URI));
    assertThat(hiveConf.getIntVar(ConfVars.METASTORETHRIFTCONNECTIONRETRIES), is(5));
    assertThat(hiveConf.getTimeVar(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS), is(6000L));
    assertThat(hiveConf.getTimeVar(ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS), is(5L));
    assertThat(hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT), is(true));
    assertThat(hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL), is(false));
  }

  @Test
  public void tunnelingFactory() {
    MetastoreTunnel metastoreTunnel = new MetastoreTunnel();
    metastoreTunnel.setLocalhost("local-machine");
    metastoreTunnel.setPort(2222);
    metastoreTunnel.setRoute("a -> b -> c");
    metastoreTunnel.setKnownHosts("knownHosts");
    metastoreTunnel.setPrivateKeys("privateKeys");
    metastoreTunnel.setTimeout(123);
    AbstractMetaStore federatedMetaStore = newFederatedInstance("fed1", THRIFT_URI);
    federatedMetaStore.setMetastoreTunnel(metastoreTunnel);

    factory.newInstance(federatedMetaStore);
    verify(tunnelingMetaStoreClientFactory)
        .newInstance(THRIFT_URI, metastoreTunnel, "fed1", 3, 2000, configurationProperties);
    verifyNoInteractions(defaultMetaStoreClientFactory);
  }

  @Test
  public void glueFactory() throws Exception {
    ArgumentCaptor<HiveConf> hiveConfCaptor = ArgumentCaptor.forClass(HiveConf.class);
    FederatedMetaStore federatedMetaStore = new FederatedMetaStore("fedGlue", null);
    GlueConfig glueConfig = new GlueConfig();
    String glueAccountId = "123456789012";
    glueConfig.setGlueAccountId(glueAccountId);
    String glueEndpoint = "glue.us-east-1.amazonaws.com";
    glueConfig.setGlueEndpoint(glueEndpoint);
    federatedMetaStore.setGlueConfig(glueConfig);

    when(glueClientFactory.newInstance(hiveConfCaptor.capture(), eq(null))).thenReturn(glueClient);
    CloseableThriftHiveMetastoreIface newInstance = factory.newInstance(federatedMetaStore);

    assertThat(newInstance, isA(MetastoreIfaceAdapter.class));
    verifyNoInteractions(tunnelingMetaStoreClientFactory, defaultMetaStoreClientFactory);
    HiveConf hiveConf = hiveConfCaptor.getValue();
    assertThat(hiveConf.get("hive.metastore.glue.catalogid"), is(glueAccountId));
    assertThat(hiveConf.get("aws.glue.endpoint"), is(glueEndpoint));
    assertThat(hiveConf.getVar(ConfVars.METASTOREURIS), is(""));
    assertThat(hiveConf.getIntVar(ConfVars.METASTORETHRIFTCONNECTIONRETRIES), is(5));
    assertThat(hiveConf.getTimeVar(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS), is(6000L));
    assertThat(hiveConf.getTimeVar(ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS), is(5L));
    assertThat(hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT), is(true));
    assertThat(hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL), is(false));
  }
}
