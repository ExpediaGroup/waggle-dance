/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
package com.hotels.bdp.waggledance.server;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.Closeable;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.thrift.TProcessor;
import org.apache.thrift.transport.TTransport;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TSetIpAddressProcessorFactoryTest {

  private @Mock FederatedHMSHandler federatedHMSHandler;
  private @Mock FederatedHMSHandlerFactory federatedHMSHandlerFactory;
  private @Mock TTransportMonitor transportMonitor;
  private @Mock TTransport transport;

  private final HiveConf hiveConf = new HiveConf();
  private TSetIpAddressProcessorFactory factory;

  @Before
  public void init() {
    when(federatedHMSHandlerFactory.create()).thenReturn(federatedHMSHandler);
    factory = new TSetIpAddressProcessorFactory(hiveConf, federatedHMSHandlerFactory, transportMonitor);
  }

  @Test
  public void correctType() throws Exception {
    TProcessor processor = factory.getProcessor(transport);
    assertThat(TSetIpAddressProcessor.class.isAssignableFrom(processor.getClass()), is(true));
  }

  @Test
  public void connectionIsMonitored() throws Exception {
    factory.getProcessor(transport);

    ArgumentCaptor<TTransport> transportCaptor = ArgumentCaptor.forClass(TTransport.class);
    ArgumentCaptor<Closeable> handlerCaptor = ArgumentCaptor.forClass(Closeable.class);
    verify(transportMonitor).monitor(transportCaptor.capture(), handlerCaptor.capture());
    assertThat(transportCaptor.getValue(), is(transport));
    assertThat(handlerCaptor.getValue(), is(instanceOf(FederatedHMSHandler.class)));
  }
}
