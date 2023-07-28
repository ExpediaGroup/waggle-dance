/**
 * Copyright (C) 2016-2023 Expedia, Inc.
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

import java.io.Closeable;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.PreDestroy;
import javax.annotation.WillClose;

import org.apache.thrift.transport.TTransport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;

@Component
@Log4j2
public class TTransportMonitor {

  @AllArgsConstructor
  private static class ActionContainer {
    private final TTransport transport;
    private final Closeable action;
  }

  private final ScheduledExecutorService scheduler;
  private final ConcurrentLinkedQueue<ActionContainer> transports = new ConcurrentLinkedQueue<>();

  @Autowired
  public TTransportMonitor(WaggleDanceConfiguration waggleDanceConfiguration) {
    this(waggleDanceConfiguration, Executors.newScheduledThreadPool(1));
  }

  @VisibleForTesting
  TTransportMonitor(WaggleDanceConfiguration waggleDanceConfiguration, ScheduledExecutorService scheduler) {
    this.scheduler = scheduler;
    Runnable monitor = () -> {
      log.debug("Releasing disconnected sessions");
      Iterator<ActionContainer> iterator = transports.iterator();
      while (iterator.hasNext()) {
        ActionContainer actionContainer = iterator.next();
        if (actionContainer.transport.peek()) {
          continue;
        }
        try {
          actionContainer.action.close();
        } catch (Exception e) {
          log.warn("Error closing action", e);
        }
        try {
          actionContainer.transport.close();
        } catch (Exception e) {
          log.warn("Error closing transport", e);
        }
        iterator.remove();
      }
    };
    this.scheduler
        .scheduleAtFixedRate(monitor, waggleDanceConfiguration.getDisconnectConnectionDelay(),
            waggleDanceConfiguration.getDisconnectConnectionDelay(), waggleDanceConfiguration.getDisconnectTimeUnit());
  }

  @PreDestroy
  public void shutdown() {
    scheduler.shutdown();
  }

  public void monitor(@WillClose TTransport transport, @WillClose Closeable action) {
    transports.offer(new ActionContainer(transport, action));
  }

}
