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
package com.hotels.bdp.waggledance.mapping.model;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Map;

import com.hotels.bdp.waggledance.util.TrackExecutionTime;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.DatabaseResolution;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIfaceClientFactory;
import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.PrefixNamingStrategy;
import com.hotels.bdp.waggledance.server.WaggleDanceServerException;
import com.hotels.bdp.waggledance.server.security.AccessControlHandlerFactory;

@Component
public class MetaStoreMappingFactoryImpl implements MetaStoreMappingFactory {
  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreMappingFactoryImpl.class);

  private final WaggleDanceConfiguration waggleDanceConfiguration;
  private final PrefixNamingStrategy prefixNamingStrategy;
  private final CloseableThriftHiveMetastoreIfaceClientFactory metaStoreClientFactory;
  private final AccessControlHandlerFactory accessControlHandlerFactory;

  @Autowired
  public MetaStoreMappingFactoryImpl(
      WaggleDanceConfiguration waggleDanceConfiguration,
      PrefixNamingStrategy prefixNamingStrategy,
      CloseableThriftHiveMetastoreIfaceClientFactory metaStoreClientFactory,
      AccessControlHandlerFactory accessControlHandlerFactory) {
    this.waggleDanceConfiguration = waggleDanceConfiguration;
    this.prefixNamingStrategy = prefixNamingStrategy;
    this.metaStoreClientFactory = metaStoreClientFactory;
    this.accessControlHandlerFactory = accessControlHandlerFactory;
  }

  @TrackExecutionTime
  private CloseableThriftHiveMetastoreIface createClient(AbstractMetaStore metaStore) {
    try {
      return metaStoreClientFactory.newInstance(metaStore);
    } catch (Exception e) {
      LOG.error("Can't create a client for metastore '{}':", metaStore.getName(), e);
      return newUnreachableMetastoreClient(metaStore);
    }
  }

  @SuppressWarnings("resource")
  @TrackExecutionTime
  @Override
  public MetaStoreMapping newInstance(AbstractMetaStore metaStore) {
    LOG
        .debug("Mapping databases with name '{}' to metastore: {}", metaStore.getName(),
            metaStore.getRemoteMetaStoreUris());
    MetaStoreMapping metaStoreMapping = new MetaStoreMappingImpl(prefixNameFor(metaStore), metaStore.getName(),
        createClient(metaStore), accessControlHandlerFactory.newInstance(metaStore), metaStore.getConnectionType(),
        metaStore.getLatency(), loadMetastoreFilterHook(metaStore));
    if (waggleDanceConfiguration.getDatabaseResolution() == DatabaseResolution.PREFIXED) {
      return new DatabaseNameMapping(new PrefixMapping(metaStoreMapping), metaStore.getDatabaseNameBiMapping());
    } else {
      return new DatabaseNameMapping(metaStoreMapping, metaStore.getDatabaseNameBiMapping());
    }
  }

  @TrackExecutionTime
  @Override
  public String prefixNameFor(AbstractMetaStore federatedMetaStore) {
    return prefixNamingStrategy.apply(federatedMetaStore);
  }

  @TrackExecutionTime
  private CloseableThriftHiveMetastoreIface newUnreachableMetastoreClient(AbstractMetaStore metaStore) {
    return (CloseableThriftHiveMetastoreIface) Proxy
        .newProxyInstance(getClass().getClassLoader(), new Class[] { CloseableThriftHiveMetastoreIface.class },
            new UnreachableMetastoreClientInvocationHandler(metaStore.getName()));
  }

  @TrackExecutionTime
  private MetaStoreFilterHook loadMetastoreFilterHook(AbstractMetaStore metaStore) {
    HiveConf conf = new HiveConf();
    String metaStoreFilterHook = metaStore.getHiveMetastoreFilterHook();
    if (metaStoreFilterHook == null || metaStoreFilterHook.isEmpty()) {
      return new DefaultMetaStoreFilterHookImpl(conf);
    }
    Map<String, String> configurationProperties = waggleDanceConfiguration.getConfigurationProperties();
    if (configurationProperties != null) {
      for (Map.Entry<String, String> property : configurationProperties.entrySet()) {
        conf.set(property.getKey(), property.getValue());
      }
    }
    conf.set(HiveConf.ConfVars.METASTORE_FILTER_HOOK.varname, metaStoreFilterHook);
    try {
      Class<? extends MetaStoreFilterHook> filterHookClass = conf
          .getClass(HiveConf.ConfVars.METASTORE_FILTER_HOOK.varname, DefaultMetaStoreFilterHookImpl.class,
              MetaStoreFilterHook.class);
      Constructor<? extends MetaStoreFilterHook> constructor = filterHookClass.getConstructor(HiveConf.class);
      return constructor.newInstance(conf);
    } catch (Exception e) {
      throw new WaggleDanceServerException("Unable to create instance of " + metaStoreFilterHook, e);
    }
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

    @TrackExecutionTime
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
