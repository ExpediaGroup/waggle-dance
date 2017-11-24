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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIfaceClientFactory;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.PrefixNamingStrategy;
import com.hotels.bdp.waggledance.server.security.AccessControlHandlerFactory;

@Component
public class MetaStoreMappingFactoryImpl implements MetaStoreMappingFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreMappingFactoryImpl.class);

  private final PrefixNamingStrategy prefixNamingStrategy;
  private final CloseableThriftHiveMetastoreIfaceClientFactory metaStoreClientFactory;
  private final AccessControlHandlerFactory accessControlHandlerFactory;

  @Autowired
  public MetaStoreMappingFactoryImpl(
      PrefixNamingStrategy prefixNamingStrategy,
      CloseableThriftHiveMetastoreIfaceClientFactory metaStoreClientFactory,
      AccessControlHandlerFactory accessControlHandlerFactory) {
    this.prefixNamingStrategy = prefixNamingStrategy;
    this.metaStoreClientFactory = metaStoreClientFactory;
    this.accessControlHandlerFactory = accessControlHandlerFactory;
  }

  private CloseableThriftHiveMetastoreIface createClient(AbstractMetaStore metaStore) {
    return metaStoreClientFactory.newInstance(metaStore);
  }

  @Override
  public MetaStoreMapping newInstance(AbstractMetaStore metaStore) {
    LOG.info("Mapping databases with name '{}' to metastore: {}", metaStore.getName(),
        metaStore.getRemoteMetaStoreUris());
    MetaStoreMapping mapping = new MetaStoreMappingImpl(prefixNameFor(metaStore), metaStore.getName(),
        createClient(metaStore), accessControlHandlerFactory.newInstance(metaStore));
    return mapping;
  }

  @Override
  public String prefixNameFor(AbstractMetaStore federatedMetaStore) {
    return prefixNamingStrategy.apply(federatedMetaStore);
  }

}
