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
package com.hotels.bdp.waggledance.server;

import java.net.Socket;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.thrift.TException;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
class TSetIpAddressProcessorFactory extends TProcessorFactory {
  private static Logger log = LoggerFactory.getLogger(TSetIpAddressProcessorFactory.class);
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
      if (transport instanceof TSocket) {
        Socket socket = ((TSocket) transport).getSocket();
        log.debug("Received a connection from ip: {}", socket.getInetAddress().getHostAddress());
      }
      CloseableIHMSHandler handler = ExceptionWrappingHMSHandler.newProxyInstance(federatedHMSHandlerFactory);
      boolean useSASL = hiveConf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL);
      if (useSASL) {
        try {
          handler.getStatus();
        } catch (TException e) {
          throw new RuntimeException("Error creating TProcessor. Could not get status.", e);
        }
      }
      transportMonitor.monitor(transport, handler);
      TSetIpAddressProcessor<CloseableIHMSHandler> result = new TSetIpAddressProcessor<>(handler);
      return result;
    } catch (ReflectiveOperationException | RuntimeException e) {
      throw new RuntimeException("Error creating TProcessor", e);
    }
  }
}
