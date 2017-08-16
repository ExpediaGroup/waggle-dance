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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetastoreTunnel;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.client.HiveConfFactory;
import com.hotels.bdp.waggledance.client.MetaStoreClientFactory;
import com.hotels.bdp.waggledance.client.SessionFactorySupplierFactory;
import com.hotels.bdp.waggledance.client.TunnelingMetaStoreClientFactory;
import com.hotels.bdp.waggledance.client.TunnelingMetastoreClientBuilder;
import com.hotels.bdp.waggledance.client.WaggleDanceHiveConfVars;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.PrefixNamingStrategy;
import com.hotels.bdp.waggledance.server.security.AccessControlHandlerFactory;

@Component
public class MetaStoreMappingFactoryImpl implements MetaStoreMappingFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreMappingFactoryImpl.class);

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

  private final PrefixNamingStrategy prefixNamingStrategy;
  private final MetaStoreClientFactory metaStoreClientFactory;
  private final AccessControlHandlerFactory accessControlHandlerFactory;

  @Autowired
  public MetaStoreMappingFactoryImpl(
      PrefixNamingStrategy prefixNamingStrategy,
      AccessControlHandlerFactory accessControlHandlerFactory) {
    this(prefixNamingStrategy,
        new TunnelingMetaStoreClientFactory(new SessionFactorySupplierFactory(), new TunnelingMetastoreClientBuilder()),
        accessControlHandlerFactory);
  }

  @VisibleForTesting
  MetaStoreMappingFactoryImpl(
      PrefixNamingStrategy prefixNamingStrategy,
      MetaStoreClientFactory metaStoreClientFactory,
      AccessControlHandlerFactory accessControlHandlerFactory) {
    this.prefixNamingStrategy = prefixNamingStrategy;
    this.metaStoreClientFactory = metaStoreClientFactory;
    this.accessControlHandlerFactory = accessControlHandlerFactory;
  }

  private CloseableThriftHiveMetastoreIface createClient(String name, String uris, MetastoreTunnel metastoreTunnel) {
    Map<String, String> properties = new HashMap<>();
    properties.put(ConfVars.METASTOREURIS.varname, uris);
    if (metastoreTunnel != null) {
      properties.put(WaggleDanceHiveConfVars.SSH_LOCALHOST.varname, metastoreTunnel.getLocalhost());
      properties.put(WaggleDanceHiveConfVars.SSH_PORT.varname, String.valueOf(metastoreTunnel.getPort()));
      properties.put(WaggleDanceHiveConfVars.SSH_ROUTE.varname, metastoreTunnel.getRoute());
      properties.put(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname, metastoreTunnel.getKnownHosts());
      properties.put(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, metastoreTunnel.getPrivateKeys());
    }
    HiveConfFactory confFactory = new HiveConfFactory(Collections.<String> emptyList(), properties);
    return metaStoreClientFactory.newInstance(confFactory.newInstance(), "waggledance-" + name, 3);
  }

  @Override
  public MetaStoreMapping newInstance(AbstractMetaStore metaStore) {
    String uris = normaliseMetaStoreUris(metaStore.getRemoteMetaStoreUris());
    String name = metaStore.getName().toLowerCase();
    LOG.info("Mapping databases with name '{}' to metastore: {}", name, uris);
    MetaStoreMapping mapping = new MetaStoreMappingImpl(prefixNameFor(metaStore), metaStore.getName(),
        createClient(name, uris, metaStore.getMetastoreTunnel()), accessControlHandlerFactory.newInstance(metaStore));
    return mapping;
  }

  @Override
  public String prefixNameFor(AbstractMetaStore federatedMetaStore) {
    return prefixNamingStrategy.apply(federatedMetaStore);
  }
}
