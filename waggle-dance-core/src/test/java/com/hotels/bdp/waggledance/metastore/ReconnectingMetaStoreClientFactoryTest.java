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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.sun.jdi.InvocationException;

import com.hotels.hcommon.hive.metastore.MetaStoreClientException;

@RunWith(MockitoJUnitRunner.class)
public class ReconnectingMetaStoreClientFactoryTest {

  private final HiveConf conf = new HiveConf();
  private final String name = "name";
  private final int retries = 5;

  private ReconnectingMetaStoreClientFactory metaStoreClientFactory;

  @Before
  public void init() {
    conf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://ip-12-34-567-891.us-east-1.compute.internal:9083");
    conf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 1);
    conf.setTimeVar(HiveConf.ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, 10L, TimeUnit.SECONDS);
    metaStoreClientFactory = spy(new ReconnectingMetaStoreClientFactory(retries));
  }

  @Test
  public void typical() {
    metaStoreClientFactory.newInstance(conf, name);
    verify(metaStoreClientFactory).getReconectingMetaStoreClientInvocationHandler(eq(conf), eq(name), eq(retries));
    verify(metaStoreClientFactory).getProxyInstance(any(ReconnectingMetaStoreClientInvocationHandler.class));
  }

  @Test(expected = MetaStoreClientException.class)
  public void failure() {
    doThrow(InvocationException.class).when(
        metaStoreClientFactory).getProxyInstance(any(
        ReconnectingMetaStoreClientInvocationHandler.class));
    metaStoreClientFactory.newInstance(conf, name);
  }

}
