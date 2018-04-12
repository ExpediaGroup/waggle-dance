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
package com.hotels.bdp.waggledance.client;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.jcraft.jsch.JSchException;
import com.pastdev.jsch.DefaultSessionFactory;
import com.pastdev.jsch.SessionFactory;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

public class SessionFactorySupplier implements Supplier<SessionFactory> {
  static final Logger LOG = LoggerFactory.getLogger(SessionFactorySupplier.class);
  private static final String PROPERTY_JSCH_STRICT_HOST_KEY_CHECKING = "StrictHostKeyChecking";

  private final int sshPort;
  private final String knownHosts;
  private final List<String> identityKeys;
  private final int sshTimeout;
  private final String strictHostKeyChecking;
  private SessionFactory sessionFactory = null;

  @Deprecated
  public SessionFactorySupplier(int sshPort, String knownHosts, List<String> identityKeys) {
    this(sshPort, knownHosts, identityKeys, 0);
  }

  public SessionFactorySupplier(int sshPort, String knownHosts, List<String> identityKeys, int sshTimeout) {
    Preconditions.checkArgument(0 <= sshPort && sshPort <= 65535, "Invalid SSH port number " + sshPort);
    Preconditions.checkArgument(sshTimeout >= 0, "Invalid SSH session timeout " + sshTimeout);
    this.sshPort = sshPort;
    this.knownHosts = knownHosts;
    this.identityKeys = ImmutableList.copyOf(identityKeys);
    this.sshTimeout = sshTimeout;
    this.strictHostKeyChecking = "yes";
  }

  public SessionFactorySupplier(
      int sshPort,
      String knownHosts,
      List<String> identityKeys,
      int sshTimeout,
      String strictHostKeyChecking) {
    Preconditions.checkArgument(0 <= sshPort && sshPort <= 65535, "Invalid SSH port number " + sshPort);
    Preconditions.checkArgument(sshTimeout >= 0, "Invalid SSH session timeout " + sshTimeout);
    strictHostKeyChecking = strictHostKeyChecking.toLowerCase();
    Preconditions.checkArgument(strictHostKeyChecking.equals("yes") || strictHostKeyChecking.equals("no"),
        "Invalid StrictHostKeyChecking setting " + strictHostKeyChecking);
    this.sshPort = sshPort;
    this.knownHosts = knownHosts;
    this.identityKeys = ImmutableList.copyOf(identityKeys);
    this.sshTimeout = sshTimeout;
    this.strictHostKeyChecking = strictHostKeyChecking;
  }

  @Override
  public SessionFactory get() {
    if (sessionFactory == null) {
      try {
        synchronized (this) {
          System.setProperty(DefaultSessionFactory.PROPERTY_JSCH_KNOWN_HOSTS_FILE, knownHosts);
          DefaultSessionFactory defaultSessionFactory = new DefaultSessionFactory();
          defaultSessionFactory.setIdentitiesFromPrivateKeys(identityKeys);
          defaultSessionFactory.setPort(sshPort);
          defaultSessionFactory.setConfig(PROPERTY_JSCH_STRICT_HOST_KEY_CHECKING, strictHostKeyChecking);
          sessionFactory = new DelegatingSessionFactory(defaultSessionFactory, sshTimeout);
          LOG.debug("Session factory created for {}@{}:{}", sessionFactory.getUsername(), sessionFactory.getHostname(),
              sessionFactory.getPort());
        }
      } catch (JSchException | RuntimeException e) {
        throw new WaggleDanceException(
            "Unable to create factory with knownHosts=" + knownHosts + " and identityKeys=" + identityKeys, e);
      }
    }
    return sessionFactory;
  }

}
