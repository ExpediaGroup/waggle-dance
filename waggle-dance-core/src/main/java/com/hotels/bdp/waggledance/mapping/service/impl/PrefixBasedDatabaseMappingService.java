/**
 * Copyright (C) 2016-2025 Expedia, Inc.
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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

import javax.validation.constraints.NotNull;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederationType;
import com.hotels.bdp.waggledance.api.model.MappedTables;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMappingImpl;
import com.hotels.bdp.waggledance.mapping.model.MetaStoreMapping;
import com.hotels.bdp.waggledance.mapping.model.QueryMapping;
import com.hotels.bdp.waggledance.mapping.service.GrammarUtils;
import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.PanopticConcurrentOperationExecutor;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationExecutor;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.mapping.service.requests.GetAllDatabasesRequest;
import com.hotels.bdp.waggledance.server.NoPrimaryMetastoreException;
import com.hotels.bdp.waggledance.util.AllowList;

public class PrefixBasedDatabaseMappingService implements MappingEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(PrefixBasedDatabaseMappingService.class);

  private static final String EMPTY_PREFIX = "";
  private final MetaStoreMappingFactory metaStoreMappingFactory;
  private final QueryMapping queryMapping;
  private final Map<String, DatabaseMapping> mappingsByPrefix;
  private final Map<String, AllowList> mappedDbByPrefix;
  private final Map<String, Map<String, AllowList>> mappedTblByPrefix;

  private DatabaseMapping primaryDatabaseMapping;

  public PrefixBasedDatabaseMappingService(
      MetaStoreMappingFactory metaStoreMappingFactory,
      List<AbstractMetaStore> initialMetastores,
      QueryMapping queryMapping) {
    this.metaStoreMappingFactory = metaStoreMappingFactory;
    this.queryMapping = queryMapping;
    mappingsByPrefix = Collections.synchronizedMap(new LinkedHashMap<>());
    mappedDbByPrefix = new ConcurrentHashMap<>();
    mappedTblByPrefix = new ConcurrentHashMap<>();
    for (AbstractMetaStore abstractMetaStore : initialMetastores) {
      add(abstractMetaStore);
    }
  }

  private void add(AbstractMetaStore metaStore) {
    MetaStoreMapping metaStoreMapping = metaStoreMappingFactory.newInstance(metaStore);

    DatabaseMapping databaseMapping = createDatabaseMapping(metaStoreMapping);

    if (metaStore.getFederationType() == PRIMARY) {
      primaryDatabaseMapping = databaseMapping;
    }

    mappingsByPrefix.put(metaStoreMapping.getDatabasePrefix(), databaseMapping);
    AllowList mappedDbAllowList = new AllowList(metaStore.getMappedDatabases());
    mappedDbByPrefix.put(metaStoreMapping.getDatabasePrefix(), mappedDbAllowList);

    List<MappedTables> mappedTables = metaStore.getMappedTables();
    if (mappedTables != null) {
      Map<String, AllowList> mappedTblByDb = new HashMap<>();
      for (MappedTables mapping : mappedTables) {
        AllowList tableAllowList = new AllowList(mapping.getMappedTables());
        mappedTblByDb.put(mapping.getDatabase(), tableAllowList);
      }
      mappedTblByPrefix.put(metaStoreMapping.getDatabasePrefix(), mappedTblByDb);
    }
  }

  private DatabaseMapping createDatabaseMapping(MetaStoreMapping metaStoreMapping) {
    return new DatabaseMappingImpl(metaStoreMapping, queryMapping);
  }

  private void remove(AbstractMetaStore metaStore) {
    if (metaStore.getFederationType() == PRIMARY) {
      primaryDatabaseMapping = null;
    }
    DatabaseMapping removed = mappingsByPrefix.remove(metaStoreMappingFactory.prefixNameFor(metaStore));
    IOUtils.closeQuietly(removed);
  }

  @Override
  public void onRegister(AbstractMetaStore metaStore) {
    // Synchronizing on the mappingsByPrefix map field so we ensure the implemented FederationEventListener methods are
    // processes sequentially
    synchronized (mappingsByPrefix) {
      if (mappingsByPrefix.containsKey(metaStore.getDatabasePrefix())) {
        throw new WaggleDanceException("MetaStore with prefix '"
            + metaStore.getDatabasePrefix()
            + "' already registered, remove old one first or update");
      }
      if (isPrimaryMetaStoreRegistered(metaStore)) {
        throw new WaggleDanceException("Primary metastore already registered, remove old one first or update");
      }
      add(metaStore);
    }
  }

  private boolean isPrimaryMetaStoreRegistered(AbstractMetaStore metaStore) {
    return (metaStore.getFederationType() == FederationType.PRIMARY) && (primaryDatabaseMapping != null);
  }

  @Override
  public void onUpdate(AbstractMetaStore oldMetaStore, AbstractMetaStore newMetaStore) {
    // Synchronizing on the mappingsByPrefix map field so we ensure the implemented FederationEventListener methods are
    // processes sequentially
    synchronized (mappingsByPrefix) {
      remove(oldMetaStore);
      add(newMetaStore);
    }
  }

  @Override
  public void onUnregister(AbstractMetaStore metaStore) {
    // Synchronizing on the mappingsByPrefix map field so we ensure the implemented FederationEventListener methods are
    // processes sequentially
    synchronized (mappingsByPrefix) {
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
    // Find a Metastore with a prefix
    synchronized (mappingsByPrefix) {
      for (Entry<String, DatabaseMapping> entry : mappingsByPrefix.entrySet()) {
        String metastorePrefix = entry.getKey();
        if (Strings.isNotBlank(metastorePrefix) && databaseName.startsWith(metastorePrefix)) {
          DatabaseMapping databaseMapping = entry.getValue();
          LOG.debug("Database Name `{}` maps to metastore with prefix `{}`", databaseName, metastorePrefix);
          return databaseMapping;
        }
      }
    }
    // Find a Metastore that has an empty prefix
    DatabaseMapping databaseMapping = mappingsByPrefix.get(EMPTY_PREFIX);
    if (databaseMapping != null) {
      LOG.debug("Database Name `{}` maps to metastore with EMPTY_PREFIX", databaseName);
      return databaseMapping;
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
  public void checkTableAllowed(String databaseName, String tableName, DatabaseMapping mapping)
    throws NoSuchObjectException {
    String databasePrefix = mapping.getDatabasePrefix();
    String transformedDbName = mapping.transformInboundDatabaseName(databaseName);
    if (!isTableAllowed(databasePrefix, transformedDbName, tableName)) {
      throw new NoSuchObjectException(String.format("%s.%s table not found in any mappings", databaseName, tableName));
    }
  }

  @Override
  public List<String> filterTables(String databaseName, List<String> tableNames, DatabaseMapping mapping) {
    List<String> allowedTables = new ArrayList<>();
    String databasePrefix = mapping.getDatabasePrefix();
    String transformedDb = mapping.transformInboundDatabaseName(databaseName);
    for (String table : tableNames) {
      if (isTableAllowed(databasePrefix, transformedDb, table)) {
        allowedTables.add(table);
      }
    }
    return allowedTables;
  }

  private boolean isTableAllowed(String databasePrefix, String database, String table) {
    Map<String, AllowList> dbToTblAllowList = mappedTblByPrefix.get(databasePrefix);
    if (dbToTblAllowList == null) {
      // Accept everything
      return true;
    }
    AllowList tblAllowList = dbToTblAllowList.get(database);
    if (tblAllowList == null) {
      // Accept everything
      return true;
    }
    return tblAllowList.contains(table);
  }

  /**
   * This run in parallel because includeInResults could potentially be slow (wait/retries) for certain slow responding
   * metastores. Using ExecutorService + Futures to maintain the order. Order is important for example when doing calls
   * like show databases, we return that grouped/ordered per metastore.
   */
  @Override
  public List<DatabaseMapping> getAvailableDatabaseMappings() {
    // TODO PD refactor/add same logic for StaticDatabaseMappingService.
    Builder<DatabaseMapping> builder = ImmutableList.builder();
    ForkJoinPool customThreadPool = new ForkJoinPool(mappingsByPrefix.size());
    try {
      synchronized (mappingsByPrefix) {
        List<Future<DatabaseMapping>> futures = new ArrayList<>();
        for (DatabaseMapping databaseMapping : mappingsByPrefix.values()) {
          futures.add(customThreadPool.submit(() -> {
            if (includeInResults(databaseMapping)) {
              return databaseMapping;
            }
            return null;
          }));

        }

        for (Future<DatabaseMapping> future : futures) {
          try {
            DatabaseMapping mapping = future.get();
            if (mapping != null) {
              builder.add(mapping);
            }
          } catch (InterruptedException e) {
            // ignore mapping
          } catch (ExecutionException e) {
            LOG.error("Can't include mapping ", e);
          }
        }
      }
    } finally {
      customThreadPool.shutdownNow();
    }
    List<DatabaseMapping> result = builder.build();
    return result;
  }

  @Override
  public List<DatabaseMapping> getAllDatabaseMappings() {
    return new ArrayList<>(mappingsByPrefix.values());
  }

  private Map<DatabaseMapping, String> databaseMappingsByDbPattern(@NotNull String databasePatterns) {
    Map<DatabaseMapping, String> mappings = new LinkedHashMap<>();
    Map<String, String> matchingPrefixes = GrammarUtils
        .selectMatchingPrefixes(mappingsByPrefix.keySet(), databasePatterns);
    for (Entry<String, String> prefixWithPattern : matchingPrefixes.entrySet()) {
      DatabaseMapping mapping = mappingsByPrefix.get(prefixWithPattern.getKey());
      if (mapping == null) {
        continue;
      }
      if (includeInResults(mapping)) {
        mappings.put(mapping, prefixWithPattern.getValue());
      }
    }
    return mappings;
  }

  private List<String> getMappedAllowedDatabases(List<String> databases, DatabaseMapping mapping) {
    List<String> mappedDatabases = new ArrayList<>();
    for (String database : databases) {
      if (isDbAllowed(mapping.getDatabasePrefix(), database)) {
        mappedDatabases.addAll(mapping.transformOutboundDatabaseNameMultiple(database));
      }
    }
    return mappedDatabases;
  }

  private boolean isDbAllowed(String databasePrefix, String database) {
    AllowList allowList = mappedDbByPrefix.get(databasePrefix);
    if (allowList == null) {
      // Accept everything
      return true;
    }
    return allowList.contains(database);
  }

  private boolean databaseAndTableAllowed(String database, String table, DatabaseMapping mapping) {
    String dbPrefix = mapping.getDatabasePrefix();
    boolean databaseAllowed = isDbAllowed(dbPrefix, database);
    boolean tableAllowed = isTableAllowed(dbPrefix, database, table);
    return databaseAllowed && tableAllowed;
  }

  @Override
  public PanopticOperationHandler getPanopticOperationHandler() {
    return new PanopticOperationHandler() {

      @Override
      public List<TableMeta> getTableMeta(String db_patterns, String tbl_patterns, List<String> tbl_types) {
        Map<DatabaseMapping, String> databaseMappingsForPattern = databaseMappingsByDbPattern(db_patterns);

        BiFunction<TableMeta, DatabaseMapping, Boolean> filter = (tableMeta, mapping) -> databaseAndTableAllowed(
            tableMeta.getDbName(), tableMeta.getTableName(), mapping);

        return super.getTableMeta(tbl_patterns, tbl_types, databaseMappingsForPattern, filter);
      }

      @Override
      public List<String> getAllDatabases(String databasePattern) {
        Map<DatabaseMapping, String> databaseMappingsForPattern = databaseMappingsByDbPattern(databasePattern);

        BiFunction<String, DatabaseMapping, Boolean> filter = (database, mapping) -> isDbAllowed(
            mapping.getDatabasePrefix(), database);

        return super.getAllDatabases(databaseMappingsForPattern, filter);
      }

      @Override
      public List<String> getAllDatabases() {
        List<DatabaseMapping> databaseMappings = getAvailableDatabaseMappings();
        List<GetAllDatabasesRequest> allRequests = new ArrayList<>();

        BiFunction<List<String>, DatabaseMapping, List<String>> filter = (
            databases,
            mapping) -> getMappedAllowedDatabases(databases, mapping);

        for (DatabaseMapping mapping : databaseMappings) {
          GetAllDatabasesRequest allDatabasesRequest = new GetAllDatabasesRequest(mapping, filter);
          allRequests.add(allDatabasesRequest);
        }
        return getPanopticOperationExecutor()
            .executeRequests(allRequests, GET_DATABASES_TIMEOUT, "Can't fetch databases: {}");
      }

      @Override
      public GetAllFunctionsResponse getAllFunctions(List<DatabaseMapping> databaseMappings) {
        GetAllFunctionsResponse allFunctions = super.getAllFunctions(databaseMappings);
        addNonPrefixedPrimaryMetastoreFunctions(allFunctions);
        return allFunctions;
      }

      /*
       * This is done to ensure we can fallback to un-prefixed UDFs (for primary Metastore only).
       */
      private void addNonPrefixedPrimaryMetastoreFunctions(GetAllFunctionsResponse allFunctions) {
        List<Function> newFunctions = new ArrayList<>();
        String primaryPrefix = primaryDatabaseMapping().getDatabasePrefix();
        if (!"".equals(primaryPrefix)) {
          if (allFunctions.isSetFunctions()) {
            for (Function function : allFunctions.getFunctions()) {
              newFunctions.add(function);
              if (function.getDbName().startsWith(primaryPrefix)) {
                Function unprefixed = new Function(function);
                // strip off the prefix
                primaryDatabaseMapping.transformInboundFunction(unprefixed);
                newFunctions.add(unprefixed);
              }
            }
            allFunctions.setFunctions(newFunctions);
          }
        }
      }

      @Override
      protected PanopticOperationExecutor getPanopticOperationExecutor() {
        return new PanopticConcurrentOperationExecutor();
      }
    };
  }

  @Override
  public void close() throws IOException {
    if (mappingsByPrefix != null) {
      for (MetaStoreMapping metaStoreMapping : mappingsByPrefix.values()) {
        metaStoreMapping.close();
      }
    }
  }

}
