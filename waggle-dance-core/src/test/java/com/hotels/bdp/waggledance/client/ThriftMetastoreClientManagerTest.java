/**
 * Copyright (C) 2016-2022 Expedia, Inc.
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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.client.HiveUgiArgsStub.TEST_ARGS;

import com.hotels.bdp.waggledance.mapping.service.TrackExecutionTimeAspectTest;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.thrift.transport.TSocket;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.util.ReflectionTestUtils;

import com.hotels.bdp.waggledance.client.compatibility.HiveCompatibleThriftHiveMetastoreIfaceFactory;

@RunWith(MockitoJUnitRunner.class)
@SpringBootTest(classes = TrackExecutionTimeAspectTest.class)
@EnableAutoConfiguration
public class ThriftMetastoreClientManagerTest {

  private final int connectionTimeout = 10;
  private final HiveConf hiveConf = new HiveConf();
  private @Mock TSocket transport;
  private @Mock HiveCompatibleThriftHiveMetastoreIfaceFactory hiveCompatibleThriftHiveMetastoreIfaceFactory;
  private ThriftMetastoreClientManager client;

  @Before
  public void init() {
    hiveConf.setVar(ConfVars.METASTOREURIS, "thrift://localhost:123");
    client = new ThriftMetastoreClientManager(hiveConf, hiveCompatibleThriftHiveMetastoreIfaceFactory,
        connectionTimeout);
    ReflectionTestUtils.setField(client, "transport", transport);
    ReflectionTestUtils.setField(client, "isConnected", true);
  }

  @Test(expected = RuntimeException.class)
  public void constructorEmptyURI() {
    hiveConf.setVar(ConfVars.METASTOREURIS, "");
    client = new ThriftMetastoreClientManager(hiveConf, hiveCompatibleThriftHiveMetastoreIfaceFactory,
        connectionTimeout);
  }

  @Test(expected = RuntimeException.class)
  public void constructorNullURI() {
    hiveConf.setVar(ConfVars.METASTOREURIS, null);
    client = new ThriftMetastoreClientManager(hiveConf, hiveCompatibleThriftHiveMetastoreIfaceFactory,
        connectionTimeout);
  }

  @Test(expected = RuntimeException.class)
  public void constructorNullURISchema() {
    hiveConf.setVar(ConfVars.METASTOREURIS, "123");
    client = new ThriftMetastoreClientManager(hiveConf, hiveCompatibleThriftHiveMetastoreIfaceFactory,
        connectionTimeout);
  }

  @Test(expected = RuntimeException.class)
  public void constructorInvalidURI() {
    hiveConf.setVar(ConfVars.METASTOREURIS, "://localhost:123");
    client = new ThriftMetastoreClientManager(hiveConf, hiveCompatibleThriftHiveMetastoreIfaceFactory,
        connectionTimeout);
  }

  @Test
  public void closedConnectionIsNeverClosed() {
    when(transport.isOpen()).thenReturn(false);
    client.close();
    verify(transport, never()).close();
  }

  @Test
  public void closeOpenedConnection() {
    when(transport.isOpen()).thenReturn(true);
    client.close();
    verify(transport).close();
  }

  @Test
  public void closeOpenedConnectionTwice() {
    when(transport.isOpen()).thenReturn(true);
    client.close();
    verify(transport).close();
    reset(transport);
    client.close();
    verify(transport, never()).close();
  }

  @Test
  public void typical() throws Exception {
    client.open(TEST_ARGS);
    client.close();
  }

  @Test(expected = RuntimeException.class)
  public void openSlowConnection() {
    client = new ThriftMetastoreClientManager(hiveConf, hiveCompatibleThriftHiveMetastoreIfaceFactory, 1);
    client.open(TEST_ARGS);
  }

}
