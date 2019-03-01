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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.slf4j.Logger;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.util.Whitelist;

public class DatabaseMappingUtils {

  public static final String MANUAL_RESOLUTION_TYPE = "MANUAL";
  public static final String PREFIXED_RESOLUTION_TYPE = "PREFIXED";
  private static final String INTERRUPTED_MESSAGE = "Interrupted ";
  private static final String SLOW_METASTORE_MESSAGE = "Metastore was slow to respond so results are omitted";
  private static final String NULL_POINTER_MESSAGE = "Got nullpointer";

  public static boolean isWhitelisted(String databasePrefix, String database, Map<String, Whitelist> mappedDbByPrefix) {
    Whitelist whitelist = mappedDbByPrefix.get(databasePrefix);
    if ((whitelist == null) || whitelist.isEmpty()) {
      // Accept everything
      return true;
    }
    return whitelist.contains(database);
  }

  public static List<String> getMappedWhitelistedDatabases(List<String> databases, DatabaseMapping mapping,
                                                           Map<String, Whitelist> mappedDbByPrefix) {
    List<String> mappedDatabases = new ArrayList<>();
    for (String database : databases) {
      if (DatabaseMappingUtils.isWhitelisted(mapping.getDatabasePrefix(), database, mappedDbByPrefix)) {
        mappedDatabases.add(mapping.transformOutboundDatabaseName(database));
      }
    }
    return mappedDatabases;
  }

  public static void shutdownExecutorService(ExecutorService executorService) {
    executorService.shutdown();
    try {
      if (!executorService.awaitTermination(200, TimeUnit.MILLISECONDS)) {
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
    }
  }

  private static List<?> getResultFromFuture(Iterator<DatabaseMapping> iterator, Future<List<?>> future)
      throws InterruptedException, ExecutionException, TimeoutException {
    DatabaseMapping mapping = iterator.next();
    long timeout = mapping.getTimeout();
    return future.get(timeout, TimeUnit.MILLISECONDS);
  }

  public static void getDatabasesFromFuture(List<Future<List<?>>> futures, Iterator<DatabaseMapping> iterator,
                                            List<String> combined, String errorMessage, Logger log) {
    for (Future<List<?>> future : futures) {
      try {
        List<String> result = (List<String>) getResultFromFuture(iterator, future);
        combined.addAll(result);
      } catch (InterruptedException e) {
        log.info(INTERRUPTED_MESSAGE);
      } catch (ExecutionException e) {
        log.warn(errorMessage, e.getMessage());
      } catch (TimeoutException e) {
        log.warn(SLOW_METASTORE_MESSAGE, e.getMessage());
      }
    }
  }

  public static void getTableMetaFromFuture(List<Future<List<?>>> futures, Iterator<DatabaseMapping> iterator,
                                            List<TableMeta> combined, String errorMessage, Logger log) {
    for (Future<List<?>> future : futures) {
      try {
        List<TableMeta> result = (List<TableMeta>) getResultFromFuture(iterator, future);
        combined.addAll(result);
      } catch (InterruptedException e) {
        log.info(INTERRUPTED_MESSAGE);
      } catch (ExecutionException e) {
        log.warn(errorMessage, e.getMessage());
      } catch (TimeoutException e) {
        log.warn(SLOW_METASTORE_MESSAGE, e.getMessage());
      }
    }
  }

  public static void getUgiFromFuture(List<Future<List<?>>> futures, Iterator<DatabaseMapping> iterator,
                                      Set<String> combined, String errorMessage, Logger log) {
    for (Future<List<?>> future : futures) {
      try {
        List<String> result = (List<String>) DatabaseMappingUtils.getResultFromFuture(iterator, future);
        combined.addAll(result);
      } catch (InterruptedException e) {
        log.info(INTERRUPTED_MESSAGE);
      } catch (ExecutionException e) {
        log.warn(errorMessage, e.getMessage());
      } catch (TimeoutException e) {
        log.warn(SLOW_METASTORE_MESSAGE, e.getMessage());
      }
    }
  }
}
