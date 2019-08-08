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
package com.hotels.bdp.waggledance.client;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.hcommon.hive.metastore.exception.MetastoreUnavailableException;

@RunWith(MockitoJUnitRunner.class)
public class DefaultMetaStoreClientFactoryTest {

  private @Mock ThriftMetastoreClientManager base;
  private @Mock Iface client;

  private final DefaultMetaStoreClientFactory factory = new DefaultMetaStoreClientFactory();
  private final static int RECONNECTION_RETRIES = 1;

  @Test
  public void isOpen() {
    when(base.isOpen()).thenReturn(true);

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, base);

    boolean result = iface.isOpen();
    assertThat(result, is(true));
    verify(base, never()).reconnect();
  }

  @Test
  public void isOpenWithReconnection() {
    when(base.isOpen()).thenReturn(false).thenReturn(true);

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, base);

    boolean result = iface.isOpen();
    assertThat(result, is(true));
    verify(base).reconnect();
  }

  @Test
  public void isOpenThrowsException() {
    when(base.isOpen()).thenThrow(new RuntimeException());

    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, base);

    boolean result = iface.isOpen();
    assertThat(result, is(false));
  }

  @Test
  public void closeNullBase() throws Exception {
    CloseableThriftHiveMetastoreIface iface = factory.newInstance("name", RECONNECTION_RETRIES, null);

    iface.close();
    verify(base, never()).close();
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
    verify(base).reconnect();
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
