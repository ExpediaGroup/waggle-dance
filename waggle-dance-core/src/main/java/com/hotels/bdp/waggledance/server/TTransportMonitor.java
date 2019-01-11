/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;

@Component
public class TTransportMonitor {

  private static final Logger LOG = LoggerFactory.getLogger(TTransportMonitor.class);

  private static class ActionContainer {
    private final TTransport transport;
    private final Closeable action;

    private ActionContainer(TTransport transport, Closeable action) {
      this.transport = transport;
      this.action = action;
    }
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
    Runnable monitor = new Runnable() {
      @Override
      public void run() {
        LOG.debug("Releasing disconnected sessions");
        Iterator<ActionContainer> iterator = transports.iterator();
        while (iterator.hasNext()) {
          ActionContainer actionContainer = iterator.next();
          if (actionContainer.transport.peek()) {
            continue;
          }
          try {
            actionContainer.action.close();
          } catch (Exception e) {
            LOG.warn("Error closing action", e);
          }
          try {
            actionContainer.transport.close();
          } catch (Exception e) {
            LOG.warn("Error closing transport", e);
          }
          iterator.remove();
        }
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
