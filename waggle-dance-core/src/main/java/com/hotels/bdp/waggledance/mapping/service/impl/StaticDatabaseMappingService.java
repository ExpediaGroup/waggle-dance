/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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

import static java.util.stream.Collectors.toList;

import static com.hotels.bdp.waggledance.api.model.FederationType.PRIMARY;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import javax.validation.constraints.NotNull;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederationType;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMappingImpl;
import com.hotels.bdp.waggledance.mapping.model.MetaStoreMapping;
import com.hotels.bdp.waggledance.mapping.model.QueryMapping;
import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.PanopticConcurrentOperationExecutor;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationExecutor;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.server.NoPrimaryMetastoreException;
import com.hotels.bdp.waggledance.util.Whitelist;

public class StaticDatabaseMappingService implements MappingEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(StaticDatabaseMappingService.class);

  private static final String PRIMARY_KEY = "";
  private final MetaStoreMappingFactory metaStoreMappingFactory;
  private final LoadingCache<String, List<String>> primaryDatabasesCache;
  private final Map<String, DatabaseMapping> mappingsByMetaStoreName;
  private final Map<String, DatabaseMapping> mappingsByDatabaseName;
  private final Map<String, List<String>> databaseMappingToDatabaseList;
  private DatabaseMapping primaryDatabaseMapping;
  private final QueryMapping queryMapping;

  public StaticDatabaseMappingService(
      MetaStoreMappingFactory metaStoreMappingFactory,
      List<AbstractMetaStore> initialMetastores,
      QueryMapping queryMapping) {
    this.metaStoreMappingFactory = metaStoreMappingFactory;
    this.queryMapping = queryMapping;
    primaryDatabasesCache = CacheBuilder
        .newBuilder()
        .expireAfterAccess(1, TimeUnit.MINUTES)
        .maximumSize(1)
        .build(new CacheLoader<String, List<String>>() {

          @Override
          public List<String> load(String key) throws Exception {
            if (primaryDatabaseMapping != null) {
              return primaryDatabaseMapping.getClient().get_all_databases();
            } else {
              return Lists.newArrayList();
            }
          }
        });

    mappingsByMetaStoreName = Collections.synchronizedMap(new LinkedHashMap<>());
    mappingsByDatabaseName = Collections.synchronizedMap(new LinkedHashMap<>());
    databaseMappingToDatabaseList = new ConcurrentHashMap<>();
    for (AbstractMetaStore federatedMetaStore : initialMetastores) {
      add(federatedMetaStore);
    }
  }

  private void add(AbstractMetaStore metaStore) {
    MetaStoreMapping metaStoreMapping = metaStoreMappingFactory.newInstance(metaStore);

    List<String> mappableDatabases = Collections.emptyList();
    if (metaStoreMapping.isAvailable()) {
      try {
        List<String> allDatabases = metaStoreMapping.getClient().get_all_databases();
        Whitelist whitelistedDatabases = new Whitelist(metaStore.getMappedDatabases());
        mappableDatabases = applyWhitelist(allDatabases, whitelistedDatabases);
      } catch (TException e) {
        LOG.error("Could not get databases for metastore {}", metaStore.getRemoteMetaStoreUris(), e);
      }
    }
    DatabaseMapping databaseMapping = createDatabaseMapping(metaStoreMapping, metaStore);
    mappableDatabases = mappableDatabases
        .stream()
        .flatMap(n -> databaseMapping.transformOutboundDatabaseNameMultiple(n).stream())
        .collect(toList());
    validateMappableDatabases(mappableDatabases, metaStore);

    if (metaStore.getFederationType() == PRIMARY) {
      validatePrimaryMetastoreDatabases(mappableDatabases);
      primaryDatabaseMapping = databaseMapping;
      primaryDatabasesCache.invalidateAll();
    } else {
      validateFederatedMetastoreDatabases(mappableDatabases, metaStoreMapping);
    }

    mappingsByMetaStoreName.put(metaStoreMapping.getMetastoreMappingName(), databaseMapping);
    addDatabaseMappings(mappableDatabases, databaseMapping);
    databaseMappingToDatabaseList.put(databaseMapping.getMetastoreMappingName(), mappableDatabases);
  }

  private void validateMappableDatabases(List<String> mappableDatabases, AbstractMetaStore metaStore) {
    int uniqueMappableDatabasesSize = new HashSet<>(mappableDatabases).size();
    if (uniqueMappableDatabasesSize != mappableDatabases.size()) {
      throw new WaggleDanceException(
          "Database clash, found duplicate database names after applying all the mappings. Check the configuration for metastore '"
              + metaStore.getName()
              + "', mappableDatabases are: '"
              + mappableDatabases.toString());
    }
  }

  private void validatePrimaryMetastoreDatabases(List<String> databases) {
    for (String database : databases) {
      if (mappingsByDatabaseName.containsKey(database)) {
        throw new WaggleDanceException("Database clash, found '"
            + database
            + "' in primary that was already mapped to a federated metastore '"
            + mappingsByDatabaseName.get(database).getMetastoreMappingName()
            + "', please remove the database from the federated metastore list it can't be"
            + " accessed via Waggle Dance");
      }
    }
  }

  private void validateFederatedMetastoreDatabases(List<String> mappableDatabases, MetaStoreMapping metaStoreMapping) {
    try {
      Set<String> allPrimaryDatabases = Sets.newHashSet(primaryDatabasesCache.get(PRIMARY_KEY));
      for (String database : mappableDatabases) {
        if (allPrimaryDatabases.contains(database.toLowerCase(Locale.ROOT))) {
          throw new WaggleDanceException("Database clash, found '"
              + database
              + "' to be mapped for the federated metastore '"
              + metaStoreMapping.getMetastoreMappingName()
              + "' already present in the primary database, please remove the database from the list it can't be"
              + " accessed via Waggle Dance");
        }
        if (mappingsByDatabaseName.containsKey(database.toLowerCase(Locale.ROOT))) {
          throw new WaggleDanceException("Database clash, found '"
              + database
              + "' to be mapped for the federated metastore '"
              + metaStoreMapping.getMetastoreMappingName()
              + "' already present in another federated database, please remove the database from the list it can't"
              + " be accessed via Waggle Dance");
        }
      }
    } catch (ExecutionException e) {
      throw new WaggleDanceException("Can't validate database clashes", e.getCause());
    }
  }

  private List<String> applyWhitelist(List<String> allDatabases, Whitelist whitelist) {
    List<String> matchedDatabases = new ArrayList<>();

    for (String database : allDatabases) {
      if (whitelist.contains(database)) {
        matchedDatabases.add(database);
      }
    }
    return matchedDatabases;
  }

  private void addDatabaseMappings(List<String> databases, DatabaseMapping databaseMapping) {
    for (String databaseName : databases) {
      mappingsByDatabaseName.put(databaseName, databaseMapping);
    }
  }

  private DatabaseMapping createDatabaseMapping(MetaStoreMapping metaStoreMapping, AbstractMetaStore metaStore) {
    return new DatabaseMappingImpl(metaStoreMapping, queryMapping);
  }

  private void remove(AbstractMetaStore metaStore) {
    if (metaStore.getFederationType() == PRIMARY) {
      primaryDatabaseMapping = null;
      primaryDatabasesCache.invalidateAll();
    }

    DatabaseMapping removed = mappingsByMetaStoreName.remove(metaStore.getName());
    String mappingName = removed.getMetastoreMappingName();
    List<String> databasesToRemove = databaseMappingToDatabaseList.get(mappingName);
    databaseMappingToDatabaseList.remove(mappingName);
    for (String database : databasesToRemove) {
      mappingsByDatabaseName.remove(database);
    }

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
  public DatabaseMapping databaseMapping(@NotNull String databaseName) throws NoSuchObjectException {
    DatabaseMapping databaseMapping = mappingsByDatabaseName.get(databaseName.toLowerCase(Locale.ROOT));
    if (databaseMapping != null) {
      LOG
          .debug("Database Name `{}` maps to metastore with name '{}'", databaseName,
              databaseMapping.getMetastoreMappingName());
      if (includeInResults(databaseMapping)) {
        return databaseMapping;
      }
    }
    LOG.debug("Database Name `{}` not mapped", databaseName);
    throw new NoSuchObjectException("Primary metastore does not have database " + databaseName);
  }

  @Override
  public void checkTable(String databaseName, String tableName, DatabaseMapping mapping) throws NoSuchObjectException {

  }

  @Override
  public List<String> filterTables(String databaseName, List<String> tableNames, DatabaseMapping mapping) {
    return null;
  }

  @Override
  public List<DatabaseMapping> getDatabaseMappings() {
    Builder<DatabaseMapping> builder = ImmutableList.builder();
    synchronized (mappingsByMetaStoreName) {
      for (DatabaseMapping databaseMapping : mappingsByMetaStoreName.values()) {
        if (includeInResults(databaseMapping)) {
          builder.add(databaseMapping);
        }
      }
    }
    return builder.build();
  }

  @Override
  public PanopticOperationHandler getPanopticOperationHandler() {
    return new PanopticOperationHandler() {

      @Override
      public List<TableMeta> getTableMeta(String db_patterns, String tbl_patterns, List<String> tbl_types) {

        BiFunction<TableMeta, DatabaseMapping, Boolean> filter = (tableMeta, mapping) -> {
          boolean isPrimary = mapping.equals(primaryDatabaseMapping);
          boolean isMapped = mappingsByDatabaseName.keySet().contains(tableMeta.getDbName());
          return isPrimary || isMapped;
        };
        Map<DatabaseMapping, String> mappingsForPattern = new LinkedHashMap<>();
        for (DatabaseMapping mapping : getDatabaseMappings()) {
          mappingsForPattern.put(mapping, db_patterns);
        }
        return super.getTableMeta(tbl_patterns, tbl_types, mappingsForPattern, filter);
      }

      @Override
      public List<String> getAllDatabases(String pattern) {
        BiFunction<String, DatabaseMapping, Boolean> filter = (database, mapping) -> mappingsByDatabaseName
            .keySet()
            .contains(database);

        Map<DatabaseMapping, String> mappingsForPattern = new LinkedHashMap<>();
        for (DatabaseMapping mapping : getDatabaseMappings()) {
          mappingsForPattern.put(mapping, pattern);
        }

        return super.getAllDatabases(mappingsForPattern, filter);
      }

      @Override
      public List<String> getAllDatabases() {
        return new ArrayList<>(mappingsByDatabaseName.keySet());
      }

      @Override
      protected PanopticOperationExecutor getPanopticOperationExecutor() {
        return new PanopticConcurrentOperationExecutor();
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
