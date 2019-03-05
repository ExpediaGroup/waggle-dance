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
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.service.requests.SetUgiRequest;

/**
 * Class responsible for handling the Hive operations that need to combine results from multiple Hive Metastores
 */
public abstract class PanopticOperationHandler {

  public static final String PREFIXED_RESOLUTION_TYPE = "PREFIXED";
  private static final Logger LOG = LoggerFactory.getLogger(PanopticOperationHandler.class);
  private static final String INTERRUPTED_MESSAGE = "Execution was interrupted: ";
  private static final String SLOW_METASTORE_MESSAGE = "Metastore was slow to respond so results are omitted";
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

  /**
   * Implements {@link HMSHandler#get_table_meta(String, String, List)} over multiple metastores
   *
   * @param databasePatterns database patterns to match
   * @param tablePatterns    table patterns to match
   * @param tableTypes       table types to match
   * @return list of table metadata
   */
  abstract public List<TableMeta> getTableMeta(String databasePatterns, String tablePatterns, List<String> tableTypes);

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
    Set<String> combined = new HashSet<>();
    ExecutorService executorService = Executors.newFixedThreadPool(databaseMappings.size());
    List<Future<List<?>>> futures = new ArrayList<>();

    for (DatabaseMapping mapping : databaseMappings) {
      SetUgiRequest setUgiRequest = new SetUgiRequest(mapping, user_name, group_names);
      futures.add(executorService.submit(setUgiRequest));
    }

    Iterator<DatabaseMapping> iterator = databaseMappings.iterator();
    try {
      Set<String> result = getUgiFromFuture(futures, iterator, LOG);
      combined.addAll(result);
    } finally {
      shutdownExecutorService(executorService);
    }
    return new ArrayList<>(combined);
  }

  protected List<String> getDatabasesFromFuture(List<Future<List<?>>> futures,
                                                Iterator<DatabaseMapping> iterator,
                                                String errorMessage, Logger log) {
    List<String> allDatabases = new LinkedList<>();
    for (Future<List<?>> future : futures) {
      try {
        List<String> result = (List<String>) getResultFromFuture(iterator, future, GET_DATABASES_TIMEOUT);
        allDatabases.addAll(result);
      } catch (InterruptedException e) {
        log.warn(INTERRUPTED_MESSAGE, e);
      } catch (ExecutionException e) {
        log.warn(errorMessage, e.getMessage());
      } catch (TimeoutException e) {
        log.warn(SLOW_METASTORE_MESSAGE);
      }
    }
    return allDatabases;
  }

  protected List<TableMeta> getTableMetaFromFuture(List<Future<List<?>>> futures,
                                                   Iterator<DatabaseMapping> iterator,
                                                   Logger log) {
    List<TableMeta> allTableMetas = new ArrayList<>();
    for (Future<List<?>> future : futures) {
      try {
        List<TableMeta> result = (List<TableMeta>) getResultFromFuture(iterator, future, GET_TABLE_META_TIMEOUT);
        allTableMetas.addAll(result);
      } catch (InterruptedException e) {
        log.warn(INTERRUPTED_MESSAGE, e);
      } catch (ExecutionException e) {
        log.warn("Got exception fetching get_table_meta: {}", e.getMessage());
      } catch (TimeoutException e) {
        log.warn(SLOW_METASTORE_MESSAGE);
      }
    }
    return allTableMetas;
  }

  private Set<String> getUgiFromFuture(List<Future<List<?>>> futures, Iterator<DatabaseMapping> iterator,
                                       Logger log) {
    Set<String> allUgis = new LinkedHashSet<>();
    for (Future<List<?>> future : futures) {
      try {
        List<String> result = (List<String>) getResultFromFuture(iterator, future, SET_UGI_TIMEOUT);
        allUgis.addAll(result);
      } catch (InterruptedException e) {
        log.warn(INTERRUPTED_MESSAGE, e);
      } catch (ExecutionException e) {
        log.warn("Got exception fetching UGI: {}", e.getMessage());
      } catch (TimeoutException e) {
        log.warn(SLOW_METASTORE_MESSAGE);
      }
    }
    return allUgis;
  }

  private List<?> getResultFromFuture(Iterator<DatabaseMapping> iterator, Future<List<?>> future,
                                      long methodTimeout)
      throws InterruptedException, ExecutionException, TimeoutException {
    DatabaseMapping mapping = iterator.next();
    long timeout = methodTimeout + mapping.getTimeout();
    return future.get(timeout, TimeUnit.MILLISECONDS);
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
