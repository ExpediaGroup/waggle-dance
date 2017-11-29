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

import java.lang.reflect.InvocationTargetException;

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.protocol.TProtocol;

class HMSHandlerProcessor implements TProcessor {
  private final IHMSHandler handler;
  private final TProcessor delegate;

  public HMSHandlerProcessor(IHMSHandler handler, TProcessor delegate) throws SecurityException, NoSuchFieldException,
      IllegalArgumentException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
    this.handler = handler;
    this.delegate = delegate;
  }

  @Override
  public boolean process(final TProtocol in, final TProtocol out) throws TException {
    try {
      return delegate.process(in, out);
    } finally {
      // The TransportMonitor will eventually close it all but there is no reason not to close everything when we
      // are done with the request.
      handler.shutdown();
    }
  }

}
