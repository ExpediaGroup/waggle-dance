/**
 * Copyright (C) 2016-2025 Expedia, Inc.
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
package com.hotels.bdp.waggledance.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.client.HiveUgiArgsStub.TEST_ARGS;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import com.hotels.hcommon.hive.metastore.exception.MetastoreUnavailableException;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMetaStoreClientFactoryTest {

  private @Mock ThriftMetastoreClientManager base;
  private @Mock Iface client;

  private final DefaultMetaStoreClientFactory factory = new DefaultMetaStoreClientFactory();
  private final static int RECONNECTION_RETRIES = 1;

  @Test
  public void isOpen() throws Exception {
    when(base.isOpen()).thenReturn(true);

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, base);

    boolean result = iface.isOpen();
    assertThat(result, is(true));
    verify(base, never()).reconnect(TEST_ARGS);
  }

  @Test
  public void isOpenWithReconnection() throws Exception {
    when(base.isOpen()).thenReturn(false).thenReturn(true);

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, base);

    boolean result = iface.isOpen();
    assertThat(result, is(true));
    verify(base).reconnect(null);
  }

  @Test
  public void isOpenThrowsException() {
    when(base.isOpen()).thenThrow(new RuntimeException());

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, base);

    boolean result = iface.isOpen();
    assertThat(result, is(false));
  }

  @Test
  public void defaultMethodCall() throws Exception {
    when(base.getClient()).thenReturn(client);
    when(client.getName()).thenReturn("ourName");

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, base);

    String result = iface.getName();
    assertThat(result, is("ourName"));
  }

  @Test
  public void defaultMethodCallThrowsTransportExceptionRetries() throws TException {
    when(base.getClient()).thenReturn(client);
    when(client.getName()).thenThrow(new TTransportException()).thenReturn("ourName");

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, base);

    String result = iface.getName();
    assertThat(result, is("ourName"));
    verify(base).open(null);
    verify(base).reconnect(null);
  }

  @Test
  public void set_ugi_before_call() throws Exception {
    when(base.getClient()).thenReturn(client);
    when(client.getName()).thenThrow(new TTransportException()).thenReturn("ourName");

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, base);
    List<String> setUgiResult = iface.set_ugi(TEST_ARGS.getUser(), TEST_ARGS.getGroups());
    assertThat(setUgiResult, is(Lists.newArrayList(TEST_ARGS.getUser())));
    String name = iface.getName();

    assertThat(name, is("ourName"));
    verify(base).open(TEST_ARGS);
    verify(base).reconnect(TEST_ARGS);
  }

  @Test
  public void set_ugi_CachedWhenClosed() throws Exception {
    when(base.isOpen()).thenReturn(false);

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, base);
    List<String> setUgiResult = iface.set_ugi(TEST_ARGS.getUser(), TEST_ARGS.getGroups());
    assertThat(setUgiResult, is(Lists.newArrayList(TEST_ARGS.getUser())));

    verify(base, never()).open(TEST_ARGS);
    verify(base, never()).reconnect(TEST_ARGS);
  }

  @Test
  public void set_ugi_CalledWhenOpen() throws Exception {
    when(base.getClient()).thenReturn(client);
    when(base.isOpen()).thenReturn(true);
    when(client.set_ugi(TEST_ARGS.getUser(), TEST_ARGS.getGroups())).thenReturn(Lists.newArrayList("users!"));

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, base);
    List<String> setUgiResult = iface.set_ugi(TEST_ARGS.getUser(), TEST_ARGS.getGroups());
    assertThat(setUgiResult, is(Lists.newArrayList("users!")));
  }

  @Test(expected = MetastoreUnavailableException.class)
  public void shutdownThrowsTransportExceptionNoRetry() throws TException {
    when(base.getClient()).thenReturn(client);
    doThrow(new TTransportException()).when(client).shutdown();

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, base);

    iface.shutdown();
  }

  @Test(expected = MetastoreUnavailableException.class)
  public void defaultMethodCallThrowsTransportExceptionNoRetriesLeft() throws TException {
    when(base.getClient()).thenReturn(client);
    when(client.getName()).thenThrow(new TTransportException());

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", 0, base);

    iface.getName();
  }

  @Test(expected = TException.class)
  public void defaultMethodCallThrowsRealException() throws TException {
    when(base.getClient()).thenReturn(client);
    when(client.getName()).thenThrow(new TException());

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, base);

    iface.getName();
  }
}
