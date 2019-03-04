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
  private static final String SLOW_METASTORE_MESSAGE = "Metastore was slow to respond so results are omitted: {}";
  private static final String NULL_POINTER_MESSAGE = "Received null pointer exception: ";

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

  public static List<String> getDatabasesFromFuture(List<Future<List<?>>> futures, Iterator<DatabaseMapping> iterator,
                                                    String errorMessage, Logger log) {
    List<String> allDatabases = new LinkedList<>();
    for (Future<List<?>> future : futures) {
      try {
        List<String> result = (List<String>) getResultFromFuture(iterator, future);
        allDatabases.addAll(result);
      } catch (InterruptedException e) {
        log.warn(INTERRUPTED_MESSAGE, e);
      } catch (ExecutionException e) {
        log.warn(errorMessage, e.getMessage());
      } catch (TimeoutException e) {
        log.warn(SLOW_METASTORE_MESSAGE, e.getMessage());
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
        List<TableMeta> result = (List<TableMeta>) getResultFromFuture(iterator, future);
        allTableMetas.addAll(result);
      } catch (InterruptedException e) {
        log.warn(INTERRUPTED_MESSAGE, e);
      } catch (ExecutionException e) {
        log.warn("Got exception fetching get_table_meta: {}", e.getMessage());
      } catch (TimeoutException e) {
        log.warn(SLOW_METASTORE_MESSAGE, e.getMessage());
      }
    }
    return allTableMetas;
  }

  public static Set<String> getUgiFromFuture(List<Future<List<?>>> futures, Iterator<DatabaseMapping> iterator,
                                             Logger log) {
    Set<String> allUgis = new LinkedHashSet<>();
    for (Future<List<?>> future : futures) {
      try {
        List<String> result = (List<String>) getResultFromFuture(iterator, future);
        allUgis.addAll(result);
      } catch (InterruptedException e) {
        log.warn(INTERRUPTED_MESSAGE, e);
      } catch (ExecutionException e) {
        log.warn("Got exception fetching UGI: {}", e.getMessage());
      } catch (TimeoutException e) {
        log.warn(SLOW_METASTORE_MESSAGE, e.getMessage());
      }
    }
    return allUgis;
  }
}
