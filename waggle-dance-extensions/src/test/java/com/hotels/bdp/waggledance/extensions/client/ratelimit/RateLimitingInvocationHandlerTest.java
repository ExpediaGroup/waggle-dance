/**
 * Copyright (C) 2016-2024 Expedia, Inc.
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
package com.hotels.bdp.waggledance.extensions.client.ratelimit;

import static com.hotels.bdp.waggledance.extensions.client.ratelimit.RateLimitingInvocationHandler.UNKNOWN_USER;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.client.ThriftClientFactory;
import com.hotels.bdp.waggledance.extensions.client.ratelimit.memory.InMemoryBucketService;
import com.hotels.bdp.waggledance.server.WaggleDanceServerException;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

@RunWith(MockitoJUnitRunner.class)
public class RateLimitingInvocationHandlerTest {

  private static final String USER = "user";
  private @Mock ThriftClientFactory thriftClientFactory;
  private @Mock CloseableThriftHiveMetastoreIface client;
  private @Mock BucketKeyGenerator bucketKeyGenerator;
  private MeterRegistry meterRegistry = new SimpleMeterRegistry();
  private BucketService bucketService = new InMemoryBucketService(new IntervallyBandwidthProvider(2, 1));
  private AbstractMetaStore metastore = AbstractMetaStore.newPrimaryInstance("name", "uri");
  private CloseableThriftHiveMetastoreIface handlerProxy;

  @Before
  public void setUp() {
    when(thriftClientFactory.newInstance(metastore)).thenReturn(client);
    when(bucketKeyGenerator.generateKey(USER)).thenReturn(USER);
    when(bucketKeyGenerator.generateKey(UNKNOWN_USER)).thenReturn(UNKNOWN_USER);
    handlerProxy = new RateLimitingClientFactory(thriftClientFactory, bucketService, bucketKeyGenerator, meterRegistry)
        .newInstance(metastore);
  }

  @Test
  public void testLimitDifferentUsers() throws Exception {

    assertTokens(2, 2);
    handlerProxy.get_table("db", "table");
    assertTokens(2, 1);

    handlerProxy.set_ugi(USER, null);
    assertTokens(2, 1);

    handlerProxy.get_table("db", "table");
    assertTokens(1, 1);

    handlerProxy.get_table("db", "table");
    assertTokens(0, 1);

    try {
      handlerProxy.get_table("db", "table");
      fail("Should have thrown exception.");
    } catch (WaggleDanceServerException e) {
      assertThat(e.getMessage(), is("[STATUS=429] Too many requests."));
    }
    
    verify(client, times(3)).get_table("db", "table");
    verify(client).set_ugi(USER, null);
    assertThat(meterRegistry.counter(Metrics.CALLS.getMetricName()).count(), is(4.0));
    assertThat(meterRegistry.counter(Metrics.ERRORS.getMetricName()).count(), is(0.0));
    assertThat(meterRegistry.counter(Metrics.EXCEEDED.getMetricName()).count(), is(1.0));
  }

  @Test
  public void testBucketExceptionStillDoCall() throws Exception {
    Table table = new Table();
    when(client.get_table("db", "table")).thenReturn(table);
    BucketService mockedBucketService = Mockito.mock(BucketService.class);
    when(mockedBucketService.getBucket(anyString())).thenThrow(new RuntimeException("Bucket exception"));
    CloseableThriftHiveMetastoreIface proxy = new RateLimitingClientFactory(thriftClientFactory, mockedBucketService, bucketKeyGenerator, meterRegistry)
        .newInstance(metastore);

    Table result = proxy.get_table("db", "table");
    assertThat(result, is(table));
    assertThat(meterRegistry.counter(Metrics.CALLS.getMetricName()).count(), is(1.0));
    assertThat(meterRegistry.counter(Metrics.ERRORS.getMetricName()).count(), is(1.0));
    assertThat(meterRegistry.counter(Metrics.EXCEEDED.getMetricName()).count(), is(0.0));

  }

  @Test
  public void testInvocationHandlerThrowsCause() throws Exception {
    when(client.get_table("db", "table")).thenThrow(new NoSuchObjectException("No such table"));
    try {
      handlerProxy.get_table("db", "table");
      fail("Should have thrown exception.");
    } catch (NoSuchObjectException e) {
      assertThat(e.getMessage(), is("No such table"));
    }
  }
  
  @Test
  public void testIgnoreSetUgi() throws Exception {
    assertTokens(2, 2);
    handlerProxy.set_ugi(USER, null);
    assertTokens(2, 2);

    verify(client).set_ugi(USER, null);
  }

  @Test
  public void testIgnoreFlushCache() throws Exception {
    assertTokens(2, 2);
    handlerProxy.flushCache();
    assertTokens(2, 2);

    verify(client).flushCache();
  }

  @Test
  public void testIgnoreIsOpen() throws Exception {
    assertTokens(2, 2);

    handlerProxy.isOpen();
    assertTokens(2, 2);

    verify(client).isOpen();
  }

  @Test
  public void testIgnoreClose() throws Exception {
    assertTokens(2, 2);
    handlerProxy.close();
    assertTokens(2, 2);

    verify(client).close();
  }

  private void assertTokens(long expectedUserTokenCount, long expectedUnknownUserTokenCount) {
    assertThat(bucketService.getBucket(USER).getAvailableTokens(), is(expectedUserTokenCount));
    assertThat(bucketService.getBucket(UNKNOWN_USER).getAvailableTokens(), is(expectedUnknownUserTokenCount));
  }
}
