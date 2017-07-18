/**
 * Copyright (C) 2016-2017 Expedia Inc.
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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.thrift.transport.TSocket;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(MockitoJUnitRunner.class)
public class ThriftMetastoreClientTest {

  private @Mock TSocket transport;

  private final HiveConf hiveConf = new HiveConf();
  private ThriftMetastoreClient client;

  @Before
  public void init() throws Exception {
    hiveConf.setVar(ConfVars.METASTOREURIS, "thrift://localhost:123");
    client = new ThriftMetastoreClient(hiveConf);
    ReflectionTestUtils.setField(client, "transport", transport);
    ReflectionTestUtils.setField(client, "isConnected", true);
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

}
