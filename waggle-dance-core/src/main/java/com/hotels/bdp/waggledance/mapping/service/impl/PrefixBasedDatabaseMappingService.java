/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

import javax.validation.constraints.NotNull;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.FederationType;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMappingImpl;
import com.hotels.bdp.waggledance.mapping.model.IdentityMapping;
import com.hotels.bdp.waggledance.mapping.model.MetaStoreMapping;
import com.hotels.bdp.waggledance.mapping.model.QueryMapping;
import com.hotels.bdp.waggledance.mapping.service.GrammarUtils;
import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.mapping.service.requests.GetAllDatabasesRequest;
import com.hotels.bdp.waggledance.server.NoPrimaryMetastoreException;
import com.hotels.bdp.waggledance.util.Whitelist;

public class PrefixBasedDatabaseMappingService implements MappingEventListener {

  private static final Logger LOG = LoggerFactory.getLogger(PrefixBasedDatabaseMappingService.class);

  private static final String EMPTY_PREFIX = "";
  private final MetaStoreMappingFactory metaStoreMappingFactory;
  private final QueryMapping queryMapping;
  private DatabaseMapping primaryDatabaseMapping;
  private Map<String, DatabaseMapping> mappingsByPrefix;
  private Map<String, Whitelist> mappedDbByPrefix;

  public PrefixBasedDatabaseMappingService(
      MetaStoreMappingFactory metaStoreMappingFactory,
      List<AbstractMetaStore> initialMetastores,
      QueryMapping queryMapping) {
    this.metaStoreMappingFactory = metaStoreMappingFactory;
    this.queryMapping = queryMapping;
    init(initialMetastores);
  }

  private void init(List<AbstractMetaStore> federatedMetaStores) {
    mappingsByPrefix = Collections.synchronizedMap(new LinkedHashMap<>());
    mappedDbByPrefix = new ConcurrentHashMap<>();
    for (AbstractMetaStore federatedMetaStore : federatedMetaStores) {
      add(federatedMetaStore);
    }
  }

  private void add(AbstractMetaStore federatedMetaStore) {
    MetaStoreMapping metaStoreMapping = metaStoreMappingFactory.newInstance(federatedMetaStore);

    if (federatedMetaStore.getFederationType() == PRIMARY) {
      primaryDatabaseMapping = createDatabaseMapping(metaStoreMapping);
      mappingsByPrefix.put(metaStoreMapping.getDatabasePrefix(), primaryDatabaseMapping);
    } else {
      mappingsByPrefix.put(metaStoreMapping.getDatabasePrefix(), createDatabaseMapping(metaStoreMapping));
      Whitelist mappedDbWhitelist;
      if (FederatedMetaStore.class.isAssignableFrom(federatedMetaStore.getClass())) {
        mappedDbWhitelist = getWhitelistedDatabases((FederatedMetaStore) federatedMetaStore);
        mappedDbByPrefix.put(metaStoreMapping.getDatabasePrefix(), mappedDbWhitelist);
      }
    }
  }

  private Whitelist getWhitelistedDatabases(FederatedMetaStore federatedMetaStore) {
    return new Whitelist(federatedMetaStore.getMappedDatabases());
  }

  private DatabaseMapping createDatabaseMapping(MetaStoreMapping metaStoreMapping) {
    if (Strings.isBlank(metaStoreMapping.getDatabasePrefix())) {
      return new IdentityMapping(metaStoreMapping);
    }
    return new DatabaseMappingImpl(metaStoreMapping, queryMapping);
  }

  private void remove(AbstractMetaStore federatedMetaStore) {
    if (federatedMetaStore.getFederationType() == PRIMARY) {
      primaryDatabaseMapping = null;
    }
    DatabaseMapping removed = mappingsByPrefix.remove(metaStoreMappingFactory.prefixNameFor(federatedMetaStore));
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
  public void onUnregister(AbstractMetaStore federatedMetaStore) {
    // Synchronizing on the mappingsByPrefix map field so we ensure the implemented FederationEventListener methods are
    // processes sequentially
    synchronized (mappingsByPrefix) {
      remove(federatedMetaStore);
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

  private boolean includeInResults(MetaStoreMapping metaStoreMapping, String prefixedDatabaseName) {
    return includeInResults(metaStoreMapping)
        && isWhitelisted(metaStoreMapping.getDatabasePrefix(),
        metaStoreMapping.transformInboundDatabaseName(prefixedDatabaseName));
  }

  @Override
  public DatabaseMapping databaseMapping(@NotNull String databaseName) {
    // Find a Metastore with a prefix
    synchronized (mappingsByPrefix) {
      Iterator<Entry<String, DatabaseMapping>> iterator = mappingsByPrefix.entrySet().iterator();
      while (iterator.hasNext()) {
        Entry<String, DatabaseMapping> entry = iterator.next();
        String metastorePrefix = entry.getKey();
        if (Strings.isNotBlank(metastorePrefix) && databaseName.startsWith(metastorePrefix)) {
          DatabaseMapping databaseMapping = entry.getValue();
          LOG.debug("Database Name `{}` maps to metastore with prefix `{}`", databaseName, metastorePrefix);
          if (includeInResults(databaseMapping, databaseName)) {
            return databaseMapping;
          }
        }
      }
    }
    // Find a Metastore that has an empty prefix
    DatabaseMapping databaseMapping = mappingsByPrefix.get(EMPTY_PREFIX);
    if (databaseMapping != null) {
      LOG.debug("Database Name `{}` maps to metastore with EMPTY_PREFIX", databaseName);
      if (includeInResults(databaseMapping, databaseName)) {
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
  public List<DatabaseMapping> getDatabaseMappings() {
    Builder<DatabaseMapping> builder = ImmutableList.builder();
    synchronized (mappingsByPrefix) {
      for (DatabaseMapping databaseMapping : mappingsByPrefix.values()) {
        if (includeInResults(databaseMapping)) {
          builder.add(databaseMapping);
        }
      }
    }
    return builder.build();
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

  private List<String> getMappedWhitelistedDatabases(List<String> databases, DatabaseMapping mapping) {
    List<String> mappedDatabases = new ArrayList<>();
    for (String database : databases) {
      if (isWhitelisted(mapping.getDatabasePrefix(), database)) {
        mappedDatabases.add(mapping.transformOutboundDatabaseName(database));
      }
    }
    return mappedDatabases;
  }

  private boolean isWhitelisted(String databasePrefix, String database) {
    Whitelist whitelist = mappedDbByPrefix.get(databasePrefix);
    if ((whitelist == null) || whitelist.isEmpty()) {
      // Accept everything
      return true;
    }
    return whitelist.contains(database);
  }

  @Override
  public PanopticOperationHandler getPanopticOperationHandler() {
    return new PanopticOperationHandler() {

      @Override
      public List<TableMeta> getTableMeta(String db_patterns, String tbl_patterns, List<String> tbl_types) {
        Map<DatabaseMapping, String> databaseMappingsForPattern = databaseMappingsByDbPattern(db_patterns);

        BiFunction<TableMeta, DatabaseMapping, Boolean> filter = (tableMeta, mapping) -> isWhitelisted(
            mapping.getDatabasePrefix(), tableMeta.getDbName());

        return super.getTableMeta(tbl_patterns, tbl_types, databaseMappingsForPattern, filter);
      }

      @Override
      public List<String> getAllDatabases(String databasePattern) {
        Map<DatabaseMapping, String> databaseMappingsForPattern = databaseMappingsByDbPattern(databasePattern);

        BiFunction<String, DatabaseMapping, Boolean> filter = (database, mapping) -> isWhitelisted(
            mapping.getDatabasePrefix(), database);

        return super.getAllDatabases(databaseMappingsForPattern, filter);
      }

      @Override
      public List<String> getAllDatabases() {
        List<String> combined = new ArrayList<>();
        List<DatabaseMapping> databaseMappings = getDatabaseMappings();
        ExecutorService executorService = Executors.newFixedThreadPool(databaseMappings.size());
        List<Future<List<String>>> futures = new ArrayList<>();

        BiFunction<List<String>, DatabaseMapping, List<String>> filter = (databases, mapping) -> getMappedWhitelistedDatabases(
            databases, mapping);

        for (DatabaseMapping mapping : databaseMappings) {
          GetAllDatabasesRequest allDatabasesRequest = new GetAllDatabasesRequest(mapping, filter);
          futures.add(executorService.submit(allDatabasesRequest));
        }

        try {
          List<String> result = getDatabasesFromFuture(futures, databaseMappings, "Can't fetch databases: {}");
          combined.addAll(result);
        } finally {
          shutdownExecutorService(executorService);
        }
        return combined;
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
