/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.waggledance.mapping.service.requests;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.slf4j.Logger;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;

public class RequestUtils {

  public static final String MANUAL_RESOLUTION_TYPE = "MANUAL";
  public static final String PREFIXED_RESOLUTION_TYPE = "PREFIXED";
  private static final String INTERRUPTED_MESSAGE = "Execution was interrupted: ";
  private static final String SLOW_METASTORE_MESSAGE = "Metastore was slow to respond so results are omitted";
  private static final long SET_UGI_TIMEOUT = 5000;
  private static final long GET_DATABASES_TIMEOUT = 6000;
  private static final long GET_TABLE_META_TIMEOUT = 400;

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

  private static List<?> getResultFromFuture(Iterator<DatabaseMapping> iterator, Future<List<?>> future,
                                             long methodTimeout)
      throws InterruptedException, ExecutionException, TimeoutException {
    DatabaseMapping mapping = iterator.next();
    long timeout = methodTimeout + mapping.getLatency();
    return future.get(timeout, TimeUnit.MILLISECONDS);
  }

  public static List<String> getDatabasesFromFuture(List<Future<List<?>>> futures, Iterator<DatabaseMapping> iterator,
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

  public static List<TableMeta> getTableMetaFromFuture(List<Future<List<?>>> futures,
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

  public static Set<String> getUgiFromFuture(List<Future<List<?>>> futures, Iterator<DatabaseMapping> iterator,
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
}
