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
package com.hotels.bdp.waggledance.mapping.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.service.requests.RequestCallable;

public class PanopticConcurrentOperationExecutor implements PanopticOperationExecutor {

  private static final String INTERRUPTED_MESSAGE = "Execution was interrupted: ";
  private static final String SLOW_METASTORE_MESSAGE = "Metastore {} was slow to respond so results are omitted";

  private final static Logger LOG = LoggerFactory.getLogger(PanopticConcurrentOperationExecutor.class);

  @Override
  public <T> List<T> executeRequests(
      List<? extends RequestCallable<List<T>>> allRequests,
      long requestTimeout,
      String errorMessage) {
    ExecutorService executorService = Executors.newFixedThreadPool(allRequests.size());
    try {
      List<T> allResults = new ArrayList<>();
      List<Future<List<T>>> futures = Collections.emptyList();
      Iterator<? extends RequestCallable<List<T>>> iterator = allRequests.iterator();

      try {
        long totalTimeout = getTotalTimeout(requestTimeout, allRequests);
        futures = executorService.invokeAll(allRequests, totalTimeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.warn("Execution was interrupted", e);
      }

      for (Future<List<T>> future : futures) {
        DatabaseMapping mapping = iterator.next().getMapping();
        List<T> result = getResultFromFuture(future, mapping.getMetastoreMappingName(), errorMessage);
        allResults.addAll(result);
      }
      return allResults;
    } finally {
      executorService.shutdownNow();
    }
  }

  private <T> List<T> getResultFromFuture(Future<List<T>> future, String metastoreMappingName, String errorMessage) {
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

  private <T> long getTotalTimeout(long requestTimeout, List<? extends RequestCallable<List<T>>> allRequests) {
    long maxLatency = Integer.MIN_VALUE;
    for (RequestCallable<List<T>> request : allRequests) {
      maxLatency = Math.max(maxLatency, request.getMapping().getLatency());
    }

    // Connection timeout should not be less than 1
    // Other implementations interpret a timeout of zero as infinite wait
    // `Future.get` currently does not do that, but this is safe if implementation changes in the future
    return Math.max(1, requestTimeout + maxLatency);
  }
}
