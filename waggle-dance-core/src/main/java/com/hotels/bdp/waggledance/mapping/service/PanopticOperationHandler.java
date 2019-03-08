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
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.service.requests.GetAllDatabasesByPatternRequest;
import com.hotels.bdp.waggledance.mapping.service.requests.GetAllDatabasesRequest;
import com.hotels.bdp.waggledance.mapping.service.requests.GetTableMetaRequest;
import com.hotels.bdp.waggledance.mapping.service.requests.SetUgiRequest;

/**
 * Class responsible for handling the Hive operations that need to combine results from multiple Hive Metastores
 */
public abstract class PanopticOperationHandler {

  protected static final long GET_DATABASES_TIMEOUT = TimeUnit.MILLISECONDS.toMillis(8000L);
  private static final Logger LOG = LoggerFactory.getLogger(PanopticOperationHandler.class);
  private static final String INTERRUPTED_MESSAGE = "Execution was interrupted: ";
  private static final String SLOW_METASTORE_MESSAGE = "Metastore {} was slow to respond so results are omitted";
  private static final long SET_UGI_TIMEOUT = TimeUnit.MILLISECONDS.toMillis(8000L);
  private static final long GET_TABLE_META_TIMEOUT = TimeUnit.MILLISECONDS.toMillis(400L);

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
    ExecutorService executorService = Executors.newFixedThreadPool(databaseMappingsForPattern.size());
    List<GetAllDatabasesByPatternRequest> allRequests = new ArrayList<>();
    long maxLatency = (long) Integer.MIN_VALUE;

    for (Entry<DatabaseMapping, String> mappingWithPattern : databaseMappingsForPattern.entrySet()) {
      DatabaseMapping mapping = mappingWithPattern.getKey();
      GetAllDatabasesByPatternRequest databasesByPatternRequest = new GetAllDatabasesByPatternRequest(
          mapping, mappingWithPattern.getValue(), filter);
      allRequests.add(databasesByPatternRequest);
      maxLatency = Math.max(maxLatency, mapping.getLatency());
    }

    long totalTimeout = Math.max(1, GET_DATABASES_TIMEOUT + maxLatency);
    List<String> result = getDatabasesByPatternFromFuture(executorService, allRequests, totalTimeout,
        "Can't fetch databases by pattern: {}");
    executorService.shutdownNow();

    return result;
  }

  /**
   * Implements {@link HMSHandler#get_table_meta(String, String, List)} over multiple metastores
   *
   * @param databasePatterns database patterns to match
   * @param tablePatterns table patterns to match
   * @param tableTypes table types to match
   * @return list of table metadata
   */
  abstract public List<TableMeta> getTableMeta(String databasePatterns, String tablePatterns, List<String> tableTypes);

  protected List<TableMeta> getTableMeta(
      String tablePatterns,
      List<String> tableTypes,
      Map<DatabaseMapping, String> databaseMappingsForPattern,
      BiFunction<TableMeta, DatabaseMapping, Boolean> filter) {
    ExecutorService executorService = Executors.newFixedThreadPool(databaseMappingsForPattern.size());
    List<GetTableMetaRequest> allRequests = new ArrayList<>();
    long maxLatency = (long) Integer.MIN_VALUE;

    for (Entry<DatabaseMapping, String> mappingWithPattern : databaseMappingsForPattern.entrySet()) {
      DatabaseMapping mapping = mappingWithPattern.getKey();
      GetTableMetaRequest tableMetaRequest = new GetTableMetaRequest(mapping,
          mappingWithPattern.getValue(), tablePatterns, tableTypes, filter);
      allRequests.add(tableMetaRequest);
      maxLatency = Math.max(maxLatency, mapping.getLatency());
    }

    long totalTimeout = Math.max(1, GET_TABLE_META_TIMEOUT + maxLatency);
    List<TableMeta> result = getTableMetaFromFuture(executorService, allRequests, totalTimeout);
    executorService.shutdownNow();

    return result;
  }

  /**
   * Implements {@link HMSHandler#set_ugi(String, List)} over multiple metastores
   *
   * @param user_name user name
   * @param group_names group names
   * @return list
   */
  public List<String> setUgi(String user_name, List<String> group_names, List<DatabaseMapping> databaseMappings) {
    // set_ugi returns the user_name that was set (on EMR at least) we just combine them all to avoid duplicates.
    // Not sure if anything uses these results. We're assuming the order doesn't matter.
    ExecutorService executorService = Executors.newFixedThreadPool(databaseMappings.size());
    List<SetUgiRequest> allRequests = new ArrayList<>();
    long maxLatency = (long) Integer.MIN_VALUE;

    for (DatabaseMapping mapping : databaseMappings) {
      SetUgiRequest setUgiRequest = new SetUgiRequest(mapping, user_name, group_names);
      allRequests.add(setUgiRequest);
      maxLatency = Math.max(maxLatency, mapping.getLatency());
    }

    long totalTimeout = Math.max(1, GET_TABLE_META_TIMEOUT + maxLatency);
    Set<String> result = getUgiFromFuture(executorService, allRequests, totalTimeout);
    executorService.shutdownNow();

    return new ArrayList<>(result);
  }

  protected List<String> getDatabasesFromFuture(ExecutorService executorService,
      List<GetAllDatabasesRequest> allRequests,
      long totalTimeout,
      String errorMessage) {
    List<String> allDatabases = new ArrayList<>();
    List<Future<List<String>>> futures = Collections.emptyList();
    Iterator<GetAllDatabasesRequest> iterator = allRequests.iterator();

    try {
      futures = executorService.invokeAll(allRequests, totalTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Execution was interrupted", e);
    }

    for (Future<List<String>> future : futures) {
      DatabaseMapping mapping = iterator.next().getMapping();
      List<String> result = getResultFromFuture(future, mapping.getMetastoreMappingName(), errorMessage);
      allDatabases.addAll(result);
    }
    return allDatabases;
  }

  private List<String> getDatabasesByPatternFromFuture(ExecutorService executorService,
      List<GetAllDatabasesByPatternRequest> allRequests,
      long totalTimeout,
      String errorMessage) {
    List<String> allDatabases = new ArrayList<>();
    List<Future<List<String>>> futures = Collections.emptyList();
    Iterator<GetAllDatabasesByPatternRequest> iterator = allRequests.iterator();

    try {
      futures = executorService.invokeAll(allRequests, totalTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Execution was interrupted", e);
    }

    for (Future<List<String>> future : futures) {
      DatabaseMapping mapping = iterator.next().getMapping();
      List<String> result = getResultFromFuture(future, mapping.getMetastoreMappingName(), errorMessage);
      allDatabases.addAll(result);
    }
    return allDatabases;
  }

  private List<TableMeta> getTableMetaFromFuture(
      ExecutorService executorService,
      List<GetTableMetaRequest> allRequests,
      long totalTimeout) {
    List<TableMeta> allTableMetas = new ArrayList<>();
    List<Future<List<TableMeta>>> futures = Collections.emptyList();
    Iterator<GetTableMetaRequest> iterator = allRequests.iterator();

    try {
      futures = executorService.invokeAll(allRequests, totalTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Execution was interrupted", e);
    }

    for (Future<List<TableMeta>> future : futures) {
      DatabaseMapping mapping = iterator.next().getMapping();
      List<TableMeta> result = getResultFromFuture(future, mapping.getMetastoreMappingName(),
          "Got exception fetching get_table_meta: {}");
      allTableMetas.addAll(result);
    }
    return allTableMetas;
  }

  private Set<String> getUgiFromFuture(
      ExecutorService executorService,
      List<SetUgiRequest> allRequests,
      long totalTimeout) {
    Set<String> allUgis = new LinkedHashSet<>();
    Iterator<SetUgiRequest> iterator = allRequests.iterator();
    List<Future<List<String>>> futures = Collections.emptyList();

    try {
      futures = executorService.invokeAll(allRequests, totalTimeout, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Execution was interrupted", e);
    }

    for (Future<List<String>> future : futures) {
      DatabaseMapping mapping = iterator.next().getMapping();
      List<String> result = getResultFromFuture(future, mapping.getMetastoreMappingName(),
          "Got exception setting UGI: {}");
      allUgis.addAll(result);
    }
    return allUgis;
  }

  private <T> List<T> getResultFromFuture(
      Future<List<T>> future,
      String metastoreMappingName,
      String errorMessage) {
    try {
      return future.get();
    } catch (InterruptedException e) {
      LOG.warn(INTERRUPTED_MESSAGE, e);
    } catch (ExecutionException e) {
      LOG.warn(errorMessage, e.getCause().getMessage());
    } catch (CancellationException e) {
      LOG.warn(SLOW_METASTORE_MESSAGE, metastoreMappingName);
    }
    return Collections.emptyList();
  }
}
