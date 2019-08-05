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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.service.requests.RequestCallable;

@RunWith(MockitoJUnitRunner.class)
public class PanopticConcurrentOperationExecutorTest {

  private final static long REQUEST_TIMEOUT = TimeUnit.MILLISECONDS.toMillis(500);

  private @Mock DatabaseMapping mapping1;
  private @Mock DatabaseMapping mapping2;
  private @Mock DatabaseMapping mapping3;

  @Before
  public void setUp() {
    when(mapping1.getMetastoreMappingName()).thenReturn("mapping1");
    when(mapping2.getMetastoreMappingName()).thenReturn("mapping2");
    when(mapping3.getMetastoreMappingName()).thenReturn("mapping3");
  }

  @Test
  public void executeRequestsInOrder() throws Exception {
    PanopticConcurrentOperationExecutor executor = new PanopticConcurrentOperationExecutor();
    List<DummyRequestCallable> allRequests = Lists
        .newArrayList(new DummyRequestCallable("call1", mapping1), new DummyRequestCallable("call2", mapping2),
            new DummyRequestCallable("call0", mapping3));
    List<String> executeRequests = executor.executeRequests(allRequests, REQUEST_TIMEOUT, "error");
    assertThat(executeRequests.size(), is(3));
    assertThat(executeRequests.get(0), is("call1"));
    assertThat(executeRequests.get(1), is("call2"));
    assertThat(executeRequests.get(2), is("call0"));
  }

  @Test
  public void executeRequestsSlowConnection() throws Exception {
    PanopticConcurrentOperationExecutor executor = new PanopticConcurrentOperationExecutor();
    DummyRequestCallable slowRequest = new DummyRequestCallable("call2", mapping2) {
      @Override
      public List<String> call() throws Exception {
        // Too slow should never be executed
        Thread.sleep(REQUEST_TIMEOUT * 10);
        return super.call();
      }
    };
    List<DummyRequestCallable> allRequests = Lists
        .newArrayList(new DummyRequestCallable("call1", mapping1), slowRequest);
    List<String> executeRequests = executor.executeRequests(allRequests, REQUEST_TIMEOUT, "error");
    assertThat(executeRequests.size(), is(1));
    assertThat(executeRequests.get(0), is("call1"));
  }

  @Test
  public void executeRequestsWaitForMaxLatency() throws Exception {
    Long mapping2Latency = TimeUnit.MILLISECONDS.toMillis(1000);
    when(mapping2.getLatency()).thenReturn(mapping2Latency);
    PanopticConcurrentOperationExecutor executor = new PanopticConcurrentOperationExecutor();
    DummyRequestCallable slowRequest = new DummyRequestCallable("call2", mapping2) {
      @Override
      public List<String> call() throws Exception {
        // Slow but within timeout+latency, should be called
        Thread.sleep(REQUEST_TIMEOUT + 100);
        return super.call();
      }
    };
    List<DummyRequestCallable> allRequests = Lists
        .newArrayList(new DummyRequestCallable("call1", mapping1), slowRequest);
    List<String> executeRequests = executor.executeRequests(allRequests, REQUEST_TIMEOUT, "error");
    assertThat(executeRequests.size(), is(2));
    assertThat(executeRequests.get(0), is("call1"));
    assertThat(executeRequests.get(1), is("call2"));
  }

  @Test
  public void executeRequestsExceptionLoggedResultsReturned() throws Exception {
    PanopticConcurrentOperationExecutor executor = new PanopticConcurrentOperationExecutor();
    DummyRequestCallable errorRequest = new DummyRequestCallable("call2", mapping2) {
      @Override
      public List<String> call() throws Exception {
        throw new RuntimeException("who you gonna call...");
      }
    };
    List<DummyRequestCallable> allRequests = Lists
        .newArrayList(new DummyRequestCallable("call1", mapping1), errorRequest,
            new DummyRequestCallable("call3", mapping3));
    List<String> executeRequests = executor.executeRequests(allRequests, REQUEST_TIMEOUT, "error in call: {}");
    assertThat(executeRequests.size(), is(2));
    assertThat(executeRequests.get(0), is("call1"));
    assertThat(executeRequests.get(1), is("call3"));
  }

  private class DummyRequestCallable implements RequestCallable<List<String>> {

    private final String callValue;
    private final DatabaseMapping mapping;

    private DummyRequestCallable(String callValue, DatabaseMapping mapping) {
      this.callValue = callValue;
      this.mapping = mapping;
    }

    @Override
    public List<String> call() throws Exception {
      return Lists.newArrayList(callValue);
    }

    @Override
    public DatabaseMapping getMapping() {
      return mapping;
    }

  }

}
