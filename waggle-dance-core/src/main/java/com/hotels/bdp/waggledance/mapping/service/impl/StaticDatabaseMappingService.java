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
package com.hotels.bdp.waggledance.mapping.service.impl;

import static com.hotels.bdp.waggledance.api.model.FederationType.PRIMARY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.validation.constraints.NotNull;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.FederationType;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.model.IdentityMapping;
import com.hotels.bdp.waggledance.mapping.model.MetaStoreMapping;
import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.server.NoPrimaryMetastoreException;

public class StaticDatabaseMappingService implements MappingEventListener {
  private static final Logger LOG = LoggerFactory.getLogger(StaticDatabaseMappingService.class);

  private static final String PRIMARY_KEY = "";

  private DatabaseMapping primaryDatabaseMapping;
  private Map<String, DatabaseMapping> mappingsByMetaStoreName;
  private Map<String, DatabaseMapping> mappingsByDatabaseName;
  private final MetaStoreMappingFactory metaStoreMappingFactory;

  private LoadingCache<String, List<String>> primaryDatabasesCache;

  public StaticDatabaseMappingService(
      MetaStoreMappingFactory metaStoreMappingFactory,
      List<AbstractMetaStore> initialMetastores) {
    this.metaStoreMappingFactory = metaStoreMappingFactory;
    primaryDatabasesCache = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.MINUTES).maximumSize(1).build(
        new CacheLoader<String, List<String>>() {

          @Override
          public List<String> load(String key) throws Exception {
            if (primaryDatabaseMapping != null) {
              return primaryDatabaseMapping.getClient().get_all_databases();
            } else {
              return Lists.newArrayList();
            }
          }
        });
    init(initialMetastores);
  }

  private void init(List<AbstractMetaStore> federatedMetaStores) {
    mappingsByMetaStoreName = new ConcurrentHashMap<>();
    mappingsByDatabaseName = new ConcurrentHashMap<>();
    for (AbstractMetaStore federatedMetaStore : federatedMetaStores) {
      add(federatedMetaStore);
    }
  }

  private void add(AbstractMetaStore metaStore) {
    MetaStoreMapping metaStoreMapping = metaStoreMappingFactory.newInstance(metaStore);
    if (metaStore.getFederationType() == PRIMARY) {
      validatePrimaryMetastoreDatabases(metaStoreMapping);
      primaryDatabaseMapping = createDatabaseMapping(metaStoreMapping);
      primaryDatabasesCache.invalidateAll();
      mappingsByMetaStoreName.put(metaStoreMapping.getMetastoreMappingName(), primaryDatabaseMapping);
    } else {
      List<String> mappableDatabases = ((FederatedMetaStore) metaStore).getMappedDatabases();
      validateFederatedMetastoreDatabases(mappableDatabases, metaStoreMapping);
      DatabaseMapping databaseMapping = createDatabaseMapping(metaStoreMapping);
      mappingsByMetaStoreName.put(metaStoreMapping.getMetastoreMappingName(), databaseMapping);
      addDatabaseMappings(mappableDatabases, databaseMapping);
    }
  }

  private void validatePrimaryMetastoreDatabases(MetaStoreMapping newPrimaryMetaStoreMapping) {
    try {
      for (String database : newPrimaryMetaStoreMapping.getClient().get_all_databases()) {
        if (mappingsByDatabaseName.containsKey(database)) {
          throw new WaggleDanceException("Database clash, found '"
              + database
              + "' in primary that was already mapped to a federated metastore '"
              + mappingsByDatabaseName.get(database).getMetastoreMappingName()
              + "', please remove the database from the federated metastore list it can't be accessed via Waggle Dance");
        }
      }
    } catch (TException e) {
      throw new WaggleDanceException("Can't validate database clashes", e);
    }
  }

  private void validateFederatedMetastoreDatabases(List<String> mappableDatabases, MetaStoreMapping metaStoreMapping) {
    try {
      Set<String> allPrimaryDatabases = Sets.newHashSet(primaryDatabasesCache.get(PRIMARY_KEY));
      for (String database : mappableDatabases) {
        if (allPrimaryDatabases.contains(database.toLowerCase())) {
          throw new WaggleDanceException("Database clash, found '"
              + database
              + "' to be mapped for the federated metastore '"
              + metaStoreMapping.getMetastoreMappingName()
              + "' already present in the primary database, please remove the database from the list it can't be accessed via Waggle Dance");
        }
        if (mappingsByDatabaseName.containsKey(database.toLowerCase())) {
          throw new WaggleDanceException("Database clash, found '"
              + database
              + "' to be mapped for the federated metastore '"
              + metaStoreMapping.getMetastoreMappingName()
              + "' already present in another federated database, please remove the database from the list it can't be accessed via Waggle Dance");
        }
      }
    } catch (ExecutionException e) {
      throw new WaggleDanceException("Can't validate database clashes", e.getCause());
    }
  }

  private void addDatabaseMappings(List<String> databases, DatabaseMapping databaseMapping) {
    for (String databaseName : databases) {
      mappingsByDatabaseName.put(databaseName.toLowerCase(), databaseMapping);
    }
  }

  private DatabaseMapping createDatabaseMapping(MetaStoreMapping metaStoreMapping) {
    return new IdentityMapping(metaStoreMapping);
  }

  private void remove(AbstractMetaStore metaStore) {
    if (metaStore.getFederationType() == PRIMARY) {
      primaryDatabaseMapping = null;
      primaryDatabasesCache.invalidateAll();
    } else {
      for (String databaseName : ((FederatedMetaStore) metaStore).getMappedDatabases()) {
        mappingsByDatabaseName.remove(databaseName.trim().toLowerCase());
      }
    }
    DatabaseMapping removed = mappingsByMetaStoreName.remove(metaStore.getName());
    IOUtils.closeQuietly(removed);
  }

  @Override
  public void onRegister(AbstractMetaStore metaStore) {
    // Synchronizing on the mappingsByMetaStoreName map field so we ensure the implemented FederationEventListener
    // methods are processed sequentially
    synchronized (mappingsByMetaStoreName) {
      if (mappingsByMetaStoreName.containsKey(metaStore.getName())) {
        throw new WaggleDanceException(
            "Metastore with name '" + metaStore.getName() + "' already registered, remove old one first or update");
      }
      if ((metaStore.getFederationType() == FederationType.PRIMARY) && (primaryDatabaseMapping != null)) {
        throw new WaggleDanceException("Primary metastore already registered, remove old one first or update");
      }
      add(metaStore);
    }
  }

  @Override
  public void onUpdate(AbstractMetaStore oldMetaStore, AbstractMetaStore newMetaStore) {
    // Synchronizing on the mappingsByMetaStoreName map field so we ensure the implemented FederationEventListener
    // methods are processed sequentially
    synchronized (mappingsByMetaStoreName) {
      remove(oldMetaStore);
      add(newMetaStore);
    }
  }

  @Override
  public void onUnregister(AbstractMetaStore metaStore) {
    // Synchronizing on the mappingsByMetaStoreName map field so we ensure the implemented FederationEventListener
    // methods are processed sequentially
    synchronized (mappingsByMetaStoreName) {
      remove(metaStore);
    }
  }

  @Override
  public DatabaseMapping primaryDatabaseMapping() {
    if (primaryDatabaseMapping == null) {
      throw new NoPrimaryMetastoreException("Waggle Dance error no primary database mapping available");
    }
    return primaryDatabaseMapping;
  }

  private boolean includeInResults(MetaStoreMapping metaStoreMapping) {
    return (metaStoreMapping != null) && metaStoreMapping.isAvailable();
  }

  @Override
  public DatabaseMapping databaseMapping(@NotNull String databaseName) {
    DatabaseMapping databaseMapping = mappingsByDatabaseName.get(databaseName.toLowerCase());
    if (databaseMapping != null) {
      LOG.debug("Database Name `{}` maps to metastore with name '{}'", databaseName,
          databaseMapping.getMetastoreMappingName());
      if (includeInResults(databaseMapping)) {
        return databaseMapping;
      }
    }
    if (primaryDatabaseMapping != null) {
      // If none found we fall back to primary one
      LOG.debug("Database Name `{}` maps to 'primary' metastore", databaseName);
      return primaryDatabaseMapping;
    }
    LOG.debug("Database Name `{}` not mapped", databaseName);
    throw new NoPrimaryMetastoreException(
        "Waggle Dance error no database mapping available tried to map database '" + databaseName + "'");
  }

  @Override
  public PanopticOperationHandler getPanopticOperationHandler() {
    return new PanopticOperationHandler() {

      @Override
      public List<TableMeta> getTableMeta(String db_patterns, String tbl_patterns, List<String> tbl_types) {
        List<TableMeta> combined = new ArrayList<>();
        try {
          for (TableMeta tableMeta : primaryDatabaseMapping.getClient().get_table_meta(db_patterns, tbl_patterns,
              tbl_types)) {
            combined.add(primaryDatabaseMapping.transformOutboundTableMeta(tableMeta));
          }
          for (DatabaseMapping mapping : mappingsByMetaStoreName.values()) {
            for (TableMeta tableMeta : mapping.getClient().get_table_meta(db_patterns, tbl_patterns, tbl_types)) {
              if (mappingsByDatabaseName.keySet().contains(tableMeta.getDbName())) {
                combined.add(mapping.transformOutboundTableMeta(tableMeta));
              }
            }
          }
        } catch (TException e) {
          LOG.warn("Got exception fetching get_table_meta: {}", e.getMessage());
        }
        return combined;
      }

      @Override
      public List<String> getAllDatabases(String pattern) {
        List<String> combined = new ArrayList<>();
        try {
          for (String database : primaryDatabaseMapping.getClient().get_databases(pattern)) {
            combined.add(primaryDatabaseMapping.transformOutboundDatabaseName(database));
          }
          for (DatabaseMapping mapping : mappingsByMetaStoreName.values()) {
            for (String database : mapping.getClient().get_databases(pattern)) {
              if (mappingsByDatabaseName.keySet().contains(database)) {
                combined.add(mapping.transformOutboundDatabaseName(database));
              }
            }
          }
        } catch (TException e) {
          LOG.warn("Can't fetch databases by pattern: {}", e.getMessage());
        }
        return combined;
      }

      @Override
      public List<String> getAllDatabases() {
        List<String> combined = new ArrayList<>();
        try {
          List<String> databases = primaryDatabasesCache.get(PRIMARY_KEY);
          for (String database : databases) {
            combined.add(primaryDatabaseMapping.transformOutboundDatabaseName(database));
          }
          combined.addAll(mappingsByDatabaseName.keySet());
        } catch (ExecutionException e) {
          LOG.warn("Can't fetch databases: {}", e.getCause().getMessage());
        }
        return combined;
      }

      @Override
      public List<String> setUgi(String user_name, List<String> group_names) {
        // set_ugi returns the user_name that was set (on EMR at least) we just combine them all to avoid duplicates.
        // Not sure if anything uses these results. We're assuming the order doesn't matter.
        Set<String> combined = new HashSet<>();
        try {
          List<String> result = primaryDatabaseMapping.getClient().set_ugi(user_name, group_names);
          combined.addAll(result);
          for (DatabaseMapping mapping : mappingsByMetaStoreName.values()) {
            List<String> federatedResult = mapping.getClient().set_ugi(user_name, group_names);
            combined.addAll(federatedResult);
          }
        } catch (TException e) {
          LOG.warn("Got exception fetching UGI: {}", e.getMessage());
        }
        return new ArrayList<>(combined);
      }
    };
  }

  @Override
  public void close() throws IOException {
    if (mappingsByMetaStoreName != null) {
      for (MetaStoreMapping metaStoreMapping : mappingsByMetaStoreName.values()) {
        metaStoreMapping.close();
      }
    }
  }

}
