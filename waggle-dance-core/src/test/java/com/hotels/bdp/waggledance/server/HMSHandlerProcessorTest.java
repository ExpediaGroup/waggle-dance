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
package com.hotels.bdp.waggledance.server;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class HMSHandlerProcessorTest {

  private @Mock IHMSHandler handler;
  private @Mock TProcessor delegate;
  private @Mock TProtocol in;
  private @Mock TProtocol out;

  @Test
  public void processHandlerIsShutdownOnTrue() throws Exception {
    HMSHandlerProcessor processor = new HMSHandlerProcessor(handler, delegate);
    when(delegate.process(in, out)).thenReturn(true);
    boolean result = processor.process(in, out);
    assertThat(result, is(true));
    verify(handler).shutdown();
  }

  @Test
  public void processHandlerIsShutdownOnFalse() throws Exception {
    HMSHandlerProcessor processor = new HMSHandlerProcessor(handler, delegate);
    when(delegate.process(in, out)).thenReturn(false);
    boolean result = processor.process(in, out);
    assertThat(result, is(false));
    verify(handler).shutdown();
  }

  @Test
  public void processHandlerIsShutdownOnException() throws Exception {
    HMSHandlerProcessor processor = new HMSHandlerProcessor(handler, delegate);
    when(delegate.process(in, out)).thenThrow(new TException());
    try {
      processor.process(in, out);
      fail("exception should have been thrown");
    } catch (TException e) {
      verify(handler).shutdown();
    }
  }

}
