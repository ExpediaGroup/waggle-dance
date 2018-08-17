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
package com.hotels.bdp.waggledance.junit;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.Channels;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.ByteStreams;

/**
 * Emulates a server that only receives packets but doesn't emit responses to any requests.
 * <p>
 * This class can be used to emulate a Graphite Carbon relay, for example.
 * </p>
 */
public class ServerSocketRule extends ExternalResource {
  private static final Logger LOG = LoggerFactory.getLogger(ServerSocketRule.class);

  private final ExecutorService executor = Executors.newSingleThreadExecutor();
  private final AsynchronousServerSocketChannel listener;
  private final InetSocketAddress address;
  private final ByteArrayOutputStream output = new ByteArrayOutputStream();
  private final ConcurrentLinkedQueue<Future<Void>> requests = new ConcurrentLinkedQueue<>();

  public ServerSocketRule() {
    try {
      listener = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(0));
      address = (InetSocketAddress) listener.getLocalAddress();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void before() throws Throwable {
    listener.accept(null, new CompletionHandler<AsynchronousSocketChannel, Void>() {
      @Override
      public void completed(AsynchronousSocketChannel channel, Void attachment) {
        listener.accept(null, this);
        handle(channel);
      }

      @Override
      public void failed(Throwable exception, Void attachment) {
        LOG.warn("Failed to process request", exception);
      }
    });
  }

  private void handle(final AsynchronousSocketChannel channel) {
    LOG.info("Submitting request");
    requests.offer(executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        synchronized (output) {
          try (InputStream input = Channels.newInputStream(channel)) {
            ByteStreams.copy(input, output);
          } catch (IOException e) {
            throw new RuntimeException("Error processing user request", e);
          }
        }
        return null;
      }
    }));
  }

  @Override
  protected void after() {
    LOG.info("Socket closing, got '{}' requests left", requests.size());
    executor.shutdown();
    try {
      executor.awaitTermination(1L, TimeUnit.SECONDS);
      listener.close();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] getOutput() {
    return waitAndgetOutput(100, TimeUnit.MILLISECONDS);
  }

  /**
   * Waits for timeout to get any requests and then flushes every request and returns the result
   *
   * @param timeout
   * @param unit
   * @return bytes received
   */
  public byte[] waitAndgetOutput(long timeout, TimeUnit unit) {
    try {
      Thread.sleep(unit.toMillis(timeout));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    awaitRequests(requests.size(), timeout, unit);
    synchronized (output) {
      return output.toByteArray();
    }
  }

  private void awaitRequests(int requestCount, long timeout, TimeUnit unit) {
    while (requestCount > 0) {
      if (requests.peek() == null) {
        throw new RuntimeException("No requests have been received");
      }
      try {
        requests.peek().get(timeout, unit);
        requests.poll();
        requestCount--;
      } catch (Exception e) {
        throw new RuntimeException("Error while waiting for request completion", e);
      }
    }
  }

  public int port() {
    return address.getPort();
  }

}
