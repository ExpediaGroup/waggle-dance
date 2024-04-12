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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.extensions.client.ratelimit.RateLimitingInvocationHandler.UNKNOWN_USER;

import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
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

@RunWith(MockitoJUnitRunner.class)
public class RateLimitingInvocationHandlerTest {

  private @Mock ThriftClientFactory thriftClientFactory;
  private @Mock CloseableThriftHiveMetastoreIface client;
  private BucketService bucketService = new InMemoryBucketService(new IntervallyBandwidthProvider(2, 1));
  private AbstractMetaStore metastore = AbstractMetaStore.newPrimaryInstance("name", "uri");
  private static final String USER = "user";

  @Test
  public void testLimitDifferentUsers() throws Exception {
    when(thriftClientFactory.newInstance(metastore)).thenReturn(client);
    CloseableThriftHiveMetastoreIface handlerProxy = new RateLimitingClientFactory(thriftClientFactory, bucketService)
        .newInstance(metastore);

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
  }

  @Test
  public void testBucketExceptionStillDoCall() throws Exception {
    when(thriftClientFactory.newInstance(metastore)).thenReturn(client);
    Table table = new Table();
    when(client.get_table("db", "table")).thenReturn(table);
    BucketService mockedBucketService = Mockito.mock(BucketService.class);
    when(mockedBucketService.getBucket(anyString())).thenThrow(new RuntimeException("Bucket exception"));
    CloseableThriftHiveMetastoreIface handlerProxy = new RateLimitingClientFactory(thriftClientFactory,
        mockedBucketService).newInstance(metastore);

    Table result = handlerProxy.get_table("db", "table");
    assertThat(result, is(table));
  }

  @Test
  public void testInvocationHandlerThrowsCause() throws Exception {
    when(thriftClientFactory.newInstance(metastore)).thenReturn(client);
    when(client.get_table("db", "table")).thenThrow(new NoSuchObjectException("No such table"));
    BucketService mockedBucketService = Mockito.mock(BucketService.class);
    when(mockedBucketService.getBucket(anyString())).thenThrow(new RuntimeException("Bucket exception"));
    CloseableThriftHiveMetastoreIface handlerProxy = new RateLimitingClientFactory(thriftClientFactory,
        mockedBucketService).newInstance(metastore);

    try {
      handlerProxy.get_table("db", "table");
      fail("Should have thrown exception.");
    } catch (NoSuchObjectException e) {
      assertThat(e.getMessage(), is("No such table"));
    }
  }

  private void assertTokens(long expectedUserTokenCount, long expectedUnknownUserTokenCount) {
    assertThat(bucketService.getBucket(USER).getAvailableTokens(), is(expectedUserTokenCount));
    assertThat(bucketService.getBucket(UNKNOWN_USER).getAvailableTokens(), is(expectedUnknownUserTokenCount));
  }
}
