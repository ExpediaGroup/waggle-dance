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
package com.hotels.bdp.waggledance.mapping.model;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.thrift.TException;
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
    try {
      return metaStoreClientFactory.newInstance(metaStore);
    } catch (Exception e) {
      LOG.error("Can't create a client for metastore '{}':", metaStore.getName(), e);
      return newUnreachableMetatstoreClient(metaStore);
    }
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

  private CloseableThriftHiveMetastoreIface newUnreachableMetatstoreClient(AbstractMetaStore metaStore) {
    return (CloseableThriftHiveMetastoreIface) Proxy.newProxyInstance(getClass().getClassLoader(),
        new Class[] { CloseableThriftHiveMetastoreIface.class },
        new UnreachableMetastoreClientInvocationHandler(metaStore.getName()));
  }

  /**
   * Handler that refuses to be open and will throw exceptions for any of the methods, serves as a dummy client if the
   * real one can't be created due to connection (i.e. tunneling) issues.
   */
  private static class UnreachableMetastoreClientInvocationHandler implements InvocationHandler {

    private final String name;

    private UnreachableMetastoreClientInvocationHandler(String name) {
      this.name = name;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      switch (method.getName()) {
      case "isOpen":
        return false;
      case "close":
        return null;
      default:
        throw new TException("Metastore '" + name + "' unavailable");
      }
    }
  }
}
