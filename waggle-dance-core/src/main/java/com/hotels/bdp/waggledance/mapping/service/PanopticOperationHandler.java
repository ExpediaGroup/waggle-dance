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
package com.hotels.bdp.waggledance.mapping.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.service.requests.GetAllDatabasesByPatternRequest;
import com.hotels.bdp.waggledance.mapping.service.requests.GetTableMetaRequest;
import com.hotels.bdp.waggledance.mapping.service.requests.SetUgiRequest;

/**
 * Class responsible for handling the Hive operations that need to combine results from multiple Hive Metastores
 */
public abstract class PanopticOperationHandler {

  public static final String PREFIXED_RESOLUTION_TYPE = "PREFIXED";
  private static final Logger LOG = LoggerFactory.getLogger(PanopticOperationHandler.class);
  private static final String INTERRUPTED_MESSAGE = "Execution was interrupted: ";
  private static final String SLOW_METASTORE_MESSAGE = "Metastore {} was slow to respond so results are omitted";
  private static final long SET_UGI_TIMEOUT = 5000;
  private static final long GET_DATABASES_TIMEOUT = 6000;
  private static final long GET_TABLE_META_TIMEOUT = 400;
  protected final String MANUAL_RESOLUTION_TYPE = "MANUAL";

  /**
   * Implements {@link HMSHandler#get_all_databases()} over multiple metastores
   *
   * @return list of all databases
   */
  public abstract List<String> getAllDatabases();

  /**
   * Implements {@link HMSHandler#get_database(String)} over multiple metastores
   *
   * @param databasePattern pattern to match
   * @return list of all databases that match the passed pattern
   */
  public abstract List<String> getAllDatabases(String databasePattern);

  protected List<String> getAllDatabases(
      Map<DatabaseMapping, String> databaseMappingsForPattern,
      BiFunction<String, DatabaseMapping, Boolean> filter) {

    List<String> combined = new ArrayList<>();

    ExecutorService executorService = Executors.newFixedThreadPool(databaseMappingsForPattern.size());
    List<Future<List<String>>> futures = new ArrayList<>();

    for (Entry<DatabaseMapping, String> mappingWithPattern : databaseMappingsForPattern.entrySet()) {
      GetAllDatabasesByPatternRequest databasesByPatternRequest = new GetAllDatabasesByPatternRequest(
          mappingWithPattern.getKey(), mappingWithPattern.getValue(), filter);
      futures.add(executorService.submit(databasesByPatternRequest));
    }

    try {
      List<String> result = getDatabasesFromFuture(futures, databaseMappingsForPattern.keySet(),
          "Can't fetch databases by pattern: {}");
      combined.addAll(result);
    } finally {
      shutdownExecutorService(executorService);
    }
    return combined;
  }

  /**
   * Implements {@link HMSHandler#get_table_meta(String, String, List)} over multiple metastores
   *
   * @param databasePatterns database patterns to match
   * @param tablePatterns    table patterns to match
   * @param tableTypes       table types to match
   * @return list of table metadata
   */
  abstract public List<TableMeta> getTableMeta(String databasePatterns, String tablePatterns, List<String> tableTypes);

  protected List<TableMeta> getTableMeta(
      String tablePatterns,
      List<String> tableTypes,
      Map<DatabaseMapping, String> databaseMappingsForPattern,
      BiFunction<TableMeta, DatabaseMapping, Boolean> filter) {
    List<TableMeta> combined = new ArrayList<>();
    ExecutorService executorService = Executors.newFixedThreadPool(databaseMappingsForPattern.size());
    List<Future<List<TableMeta>>> futures = new ArrayList<>();

    for (Entry<DatabaseMapping, String> mappingWithPattern : databaseMappingsForPattern.entrySet()) {
      GetTableMetaRequest tableMetaRequest = new GetTableMetaRequest(mappingWithPattern.getKey(),
          mappingWithPattern.getValue(), tablePatterns, tableTypes, filter);
      futures.add(executorService.submit(tableMetaRequest));
    }

    try {
      List<TableMeta> result = getTableMetaFromFuture(futures, databaseMappingsForPattern.keySet());
      combined.addAll(result);
    } finally {
      shutdownExecutorService(executorService);
    }
    return combined;
  }

  /**
   * Implements {@link HMSHandler#set_ugi(String, List)} over multiple metastores
   *
   * @param user_name   user name
   * @param group_names group names
   * @return list
   */
  public List<String> setUgi(String user_name, List<String> group_names, List<DatabaseMapping> databaseMappings) {
    // set_ugi returns the user_name that was set (on EMR at least) we just combine them all to avoid duplicates.
    // Not sure if anything uses these results. We're assuming the order doesn't matter.
    ExecutorService executorService = Executors.newFixedThreadPool(databaseMappings.size());
    List<Future<List<String>>> futures = new ArrayList<>();

    for (DatabaseMapping mapping : databaseMappings) {
      SetUgiRequest setUgiRequest = new SetUgiRequest(mapping, user_name, group_names);
      futures.add(executorService.submit(setUgiRequest));
    }

    try {
      Set<String> result = getUgiFromFuture(futures, databaseMappings);
      return new ArrayList<>(result);
    } finally {
      shutdownExecutorService(executorService);
    }
  }

  protected List<String> getDatabasesFromFuture(
      List<Future<List<String>>> futures,
      Collection<DatabaseMapping> databaseMappings,
      String errorMessage) {
    List<String> allDatabases = new ArrayList<>();
    Iterator<DatabaseMapping> iterator = databaseMappings.iterator();
    for (Future<List<String>> future : futures) {
      DatabaseMapping databaseMapping = iterator.next();
      List<String> result = getResultFromFuture(databaseMapping, future, GET_DATABASES_TIMEOUT, errorMessage);
      allDatabases.addAll(result);
    }
    return allDatabases;
  }

  private List<TableMeta> getTableMetaFromFuture(
      List<Future<List<TableMeta>>> futures,
      Collection<DatabaseMapping> databaseMappings) {
    List<TableMeta> allTableMetas = new ArrayList<>();
    Iterator<DatabaseMapping> iterator = databaseMappings.iterator();
    for (Future<List<TableMeta>> future : futures) {
      DatabaseMapping databaseMapping = iterator.next();
      List<TableMeta> result = getResultFromFuture(databaseMapping, future, GET_TABLE_META_TIMEOUT,
          "Got exception fetching get_table_meta: {}");
      allTableMetas.addAll(result);
    }
    return allTableMetas;
  }

  private Set<String> getUgiFromFuture(
      List<Future<List<String>>> futures,
      Collection<DatabaseMapping> databaseMappings) {
    Set<String> allUgis = new LinkedHashSet<>();
    Iterator<DatabaseMapping> iterator = databaseMappings.iterator();
    for (Future<List<String>> future : futures) {
      DatabaseMapping databaseMapping = iterator.next();
      List<String> result = getResultFromFuture(databaseMapping, future, SET_UGI_TIMEOUT,
          "Got exception setting UGI: {}");
      allUgis.addAll(result);
    }
    return allUgis;
  }

  private <T> List<T> getResultFromFuture(
      DatabaseMapping mapping,
      Future<List<T>> future,
      long methodTimeout,
      String errorMessage) {
    long timeout = methodTimeout + mapping.getLatency();
    try {
      return future.get(timeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn(INTERRUPTED_MESSAGE, e);
    } catch (ExecutionException e) {
      LOG.warn(errorMessage, e.getCause().getMessage());
    } catch (TimeoutException e) {
      LOG.warn(SLOW_METASTORE_MESSAGE, mapping.getMetastoreMappingName());
    }
    return Collections.emptyList();
  }

  protected void shutdownExecutorService(ExecutorService executorService) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(200, TimeUnit.MILLISECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
    }
  }
}
