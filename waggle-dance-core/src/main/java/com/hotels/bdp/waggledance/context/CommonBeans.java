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
package com.hotels.bdp.waggledance.context;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.springframework.context.annotation.Bean;

import com.google.common.base.Joiner;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetastoreTunnel;
import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import com.hotels.bdp.waggledance.mapping.model.ASTQueryMapping;
import com.hotels.bdp.waggledance.mapping.model.QueryMapping;
import com.hotels.bdp.waggledance.mapping.service.PrefixNamingStrategy;
import com.hotels.bdp.waggledance.mapping.service.impl.LowerCasePrefixNamingStrategy;
import com.hotels.hcommon.hive.metastore.client.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.DefaultMetaStoreClientSupplier;
import com.hotels.hcommon.hive.metastore.client.MetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.ThriftMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.tunnel.TunnelingMetaStoreClientSupplierBuilder;

@org.springframework.context.annotation.Configuration
public class CommonBeans {

  @Bean
  public HiveConf hiveConf(WaggleDanceConfiguration waggleDanceConfiguration) {
    Map<String, String> confProps = waggleDanceConfiguration.getConfigurationProperties();

    final HiveConf hiveConf = new HiveConf(new Configuration(false), getClass());
    // set all properties specified on the command line
    if (confProps != null) {
      for (Map.Entry<String, String> entry : confProps.entrySet()) {
        hiveConf.set(entry.getKey(), entry.getValue());
      }
    }
    return hiveConf;
  }

  @Bean
  public PrefixNamingStrategy prefixNamingStrategy(WaggleDanceConfiguration waggleDanceConfiguration) {
    return new LowerCasePrefixNamingStrategy();
  }

  @Bean
  MetaStoreClientFactory thriftMetaStoreClientFactory() {
    return new ThriftMetaStoreClientFactory();
  }

  @Bean
  CloseableMetaStoreClient newInstance(AbstractMetaStore metaStore, HiveConf hiveConf, MetaStoreClientFactory metaStoreClientFactory) {
    Map<String, String> properties = new HashMap<>();
    String uris = normaliseMetaStoreUris(metaStore.getRemoteMetaStoreUris());
    String name = metaStore.getName().toLowerCase();
    MetastoreTunnel metastoreTunnel = metaStore.getMetastoreTunnel();
    properties.put(HiveConf.ConfVars.METASTOREURIS.varname, uris);

    if (metastoreTunnel != null) {
      return new TunnelingMetaStoreClientSupplierBuilder()
          .withName(name)
          .withRoute(metastoreTunnel.getRoute())
          .withKnownHosts(metastoreTunnel.getKnownHosts())
          .withLocalHost(metastoreTunnel.getLocalhost())
          .withPort(metastoreTunnel.getPort())
          .withPrivateKeys(metastoreTunnel.getPrivateKeys())
          .withTimeout(metastoreTunnel.getTimeout())
          .withStrictHostKeyChecking(metastoreTunnel.getStrictHostKeyChecking())
          .build(hiveConf, metaStoreClientFactory).get();
    } else {
      return new DefaultMetaStoreClientSupplier(hiveConf, name, metaStoreClientFactory).get();
    }
  }

  private static String normaliseMetaStoreUris(String metaStoreUris) {
    try {
      String[] rawUris = metaStoreUris.split(",");
      Set<String> uris = new TreeSet<>();
      for (String rawUri : rawUris) {
        URI uri = new URI(rawUri);
        uris.add(uri.toString());
      }
      return Joiner.on(",").join(uris);
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Bean
  public QueryMapping queryMapping() {
    return ASTQueryMapping.INSTANCE;
  }

}
