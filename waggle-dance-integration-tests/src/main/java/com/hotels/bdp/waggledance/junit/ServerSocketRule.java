/**
 * Copyright (C) 2016-2018 Expedia, Inc.
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
import java.nio.channels.Channels;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

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

  private final InetSocketAddress address;
  private final ByteArrayOutputStream output = new ByteArrayOutputStream();

  private final ServerSocketChannel serverSocketChannel;

  private int requests = 0;

  public ServerSocketRule() {
    try {
      serverSocketChannel = (ServerSocketChannel) ServerSocketChannel
          .open()
          .bind(new InetSocketAddress(0))
          .configureBlocking(false);
      address = (InetSocketAddress) serverSocketChannel.getLocalAddress();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void after() {
    LOG.info("Socket closing, handled {} requests", requests);
    try {
      serverSocketChannel.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] getOutput() {
    try {
      SocketChannel socketChannel = serverSocketChannel.accept();
      while (socketChannel != null) {
        requests++;
        try (InputStream input = Channels.newInputStream(socketChannel)) {
          ByteStreams.copy(input, output);
        }
        socketChannel = serverSocketChannel.accept();
      }
    } catch (IOException e) {
      throw new RuntimeException("Error processing user request", e);
    }
    return output.toByteArray();
  }

  public int port() {
    return address.getPort();
  }

}
