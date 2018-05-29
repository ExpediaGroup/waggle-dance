/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.waggledance.metastore;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.transport.TTransportException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.hcommon.hive.metastore.MetaStoreUnavailableException;

@RunWith(MockitoJUnitRunner.class)
public class ReconnectingMetaStoreClientInvocationHandlerTest {

  private ReconnectingMetaStoreClientInvocationHandler invocationHandler;
  private @Mock ReconnectingThriftMetaStoreClient base;
  private @Mock ThriftHiveMetastore.Iface client;
  private final String name = "test-client";
  private final int maxRetries = 1;

  @Before
  public void init() {
    invocationHandler = new ReconnectingMetaStoreClientInvocationHandler(base, name, maxRetries);
    when(base.getClient()).thenReturn(client);
  }

  @Test
  public void attemptToOpenConnectionWhenAlreadyConnectedDoesntReconnect() throws Throwable {
    Class<?> clazz = Class.forName(
        ReconnectingThriftMetaStoreClient.class.getName());
    Method method = clazz.getMethod("isOpen");
    when(base.isOpen()).thenReturn(true);
    assertTrue((Boolean) invocationHandler.invoke(null, method, null));
    verify(base, times(2)).isOpen();
    verify(base, times(0)).reconnect();
  }

  @Test
  public void attemptToOpenConnectionWhenNotConnectedReconnects() throws Throwable {
    Class<?> clazz = Class.forName(
        ReconnectingThriftMetaStoreClient.class.getName());
    Method method = clazz.getMethod("isOpen");
    when(base.isOpen()).thenReturn(false).thenReturn(true);
    assertTrue((Boolean) invocationHandler.invoke(null, method, null));
    verify(base, times(2)).isOpen();
    verify(base, times(1)).reconnect();
  }

  @Test
  public void closeClient() throws Throwable {
    Class<?> clazz = Class.forName(
        ReconnectingThriftMetaStoreClient.class.getName());
    Method method = clazz.getMethod("close");
    invocationHandler.invoke(null, method, null);
    verify(base).close();
  }

  @Test
  public void invokeUnderlyingThriftHiveMetaStoreIFaceMethod() throws Throwable {
    Class<?> clazz = Class.forName(ThriftHiveMetastore.Iface.class.getName());
    Method method = clazz.getMethod("create_database", Database.class);
    Database database = new Database();
    invocationHandler.invoke(null, method, new Database[] { database });
    verify(client).create_database(eq(database));
  }

  @Test(expected = RuntimeException.class)
  public void invokeWithNonTTransportExceptionGetsThrown() throws Throwable {
    Class<?> clazz = Class.forName(ThriftHiveMetastore.Iface.class.getName());
    Method method = clazz.getMethod("create_database", Database.class);
    Database database = new Database();
    doThrow(new RuntimeException()).when(client).create_database(eq(database));
    invocationHandler.invoke(null, method, new Database[] { database });
  }

  @Test
  public void invokeWithTTransportExceptionAndRetryLessThanLimit() throws Throwable {
    Class<?> clazz = Class.forName(ThriftHiveMetastore.Iface.class.getName());
    Method method = clazz.getMethod("create_database", Database.class);
    Database database = new Database();
    doThrow(new TTransportException()).doNothing().when(client).create_database(eq(database));
    invocationHandler.invoke(null, method, new Database[] { database });
    verify(base).reconnect();
    verify(client, times(2)).create_database(eq(database));
  }

  @Test(expected = MetaStoreUnavailableException.class)
  public void invokeWithTTransportExceptionAndRetriesExceedingLimit() throws Throwable {
    Class<?> clazz = Class.forName(ThriftHiveMetastore.Iface.class.getName());
    Method method = clazz.getMethod("create_database", Database.class);
    Database database = new Database();
    doThrow(new TTransportException()).doThrow(new TTransportException()).when(client).create_database(eq(database));
    invocationHandler.invoke(null, method, new Database[] { database });
  }
}
