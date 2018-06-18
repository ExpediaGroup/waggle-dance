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
package com.hotels.bdp.waggledance.server;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TTransport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hotels.hcommon.hive.metastore.client.api.CloseableIHMSHandler;

@Component
class TSetIpAddressProcessorFactory extends TProcessorFactory {

  private final HiveConf hiveConf;
  private final FederatedHMSHandlerFactory federatedHMSHandlerFactory;
  private final TTransportMonitor transportMonitor;

  @Autowired
  public TSetIpAddressProcessorFactory(
      HiveConf hiveConf,
      FederatedHMSHandlerFactory federatedHMSHandlerFactory,
      TTransportMonitor transportMonitor) {
    super(null);
    this.hiveConf = hiveConf;
    this.federatedHMSHandlerFactory = federatedHMSHandlerFactory;
    this.transportMonitor = transportMonitor;
  }

  @Override
  public TProcessor getProcessor(TTransport transport) {
    try {
      CloseableIHMSHandler baseHandler = federatedHMSHandlerFactory.create();
      IHMSHandler handler = newRetryingHMSHandler(ExceptionWrappingHMSHandler.newProxyInstance(baseHandler), hiveConf,
          false);
      transportMonitor.monitor(transport, baseHandler);
      return new TSetIpAddressProcessor<>(handler);
    } catch (MetaException | ReflectiveOperationException | RuntimeException e) {
      throw new RuntimeException("Error creating TProcessor", e);
    }
  }

  private IHMSHandler newRetryingHMSHandler(IHMSHandler baseHandler, HiveConf hiveConf, boolean local)
    throws MetaException {
    return RetryingHMSHandler.getProxy(hiveConf, baseHandler, local);
  }

}
