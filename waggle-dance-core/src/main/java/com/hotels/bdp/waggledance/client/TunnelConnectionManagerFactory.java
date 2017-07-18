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
package com.hotels.bdp.waggledance.client;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.net.ServerSocket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.pastdev.jsch.SessionFactory;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

public class TunnelConnectionManagerFactory {
  private static final Logger LOG = LoggerFactory.getLogger(TunnelConnectionManagerFactory.class);

  public static final int FIRST_AVAILABLE_PORT = 0;

  private final Supplier<SessionFactory> sessionFactorySupplier;

  public TunnelConnectionManagerFactory(Supplier<SessionFactory> sessionFactorySupplier) {
    this.sessionFactorySupplier = sessionFactorySupplier;
  }

  private static int getLocalPort() {
    try (ServerSocket socket = new ServerSocket(FIRST_AVAILABLE_PORT)) {
      return socket.getLocalPort();
    } catch (IOException | RuntimeException e) {
      throw new WaggleDanceException("Unable to bind to a free localhost port", e);
    }
  }

  public TunnelConnectionManager create(String remoteHost, int remotePort) {
    return create(null, remoteHost, remotePort);
  }

  public TunnelConnectionManager create(String hops, String remoteHost, int remotePort) {
    return create(null, "localhost", FIRST_AVAILABLE_PORT, remoteHost, remotePort);
  }

  public TunnelConnectionManager create(String localHost, int localPort, String remoteHost, int remotePort) {
    return create(null, localHost, localPort, remoteHost, remotePort);
  }

  public TunnelConnectionManager create(String route, String localHost, int localPort, String remoteHost,
      int remotePort) {
    checkArgument(0 <= localPort && localPort <= 65535,
        "localPort must a valid port number, a value between 0 and 65535");
    checkArgument(0 < remotePort && remotePort <= 65535,
        "remotePort must a valid port number, a value between 1 and 65535");

    if (localPort == FIRST_AVAILABLE_PORT) {
      localPort = getLocalPort();
    }

    StringBuilder tunnelExpressionBuilder = new StringBuilder(100);
    if (Strings.isNullOrEmpty(route)) {
      tunnelExpressionBuilder.append(localHost).append("->").append(remoteHost);
    } else {
      tunnelExpressionBuilder.append(route.replaceAll("\\s", ""));
    }
    String tunnelExpression = tunnelExpressionBuilder
        .append("|")
        .append(localHost)
        .append(":")
        .append(localPort)
        .append(":")
        .append(remoteHost)
        .append(":")
        .append(remotePort)
        .toString();

    try {
      LOG.debug("Creating SSH tunnel connection manager for expression {}", tunnelExpression);
      return new TunnelConnectionManager(sessionFactorySupplier.get(), tunnelExpression);
    } catch (Exception e) {
      throw new WaggleDanceException("Unable to create a TunnelConnectionManager: " + tunnelExpression, e);
    } finally {
      LOG.debug("SSH tunnel connection manager for expression {} has been created", tunnelExpression);
    }
  }

}
