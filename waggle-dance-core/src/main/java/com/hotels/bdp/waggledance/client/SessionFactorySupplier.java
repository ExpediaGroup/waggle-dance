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
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Proxy;
import com.jcraft.jsch.Session;
import com.pastdev.jsch.DefaultSessionFactory;
import com.pastdev.jsch.SessionFactory;
import com.pastdev.jsch.SessionFactory.SessionFactoryBuilder;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

public class SessionFactorySupplier implements Supplier<SessionFactory> {
  private static final Logger LOG = LoggerFactory.getLogger(SessionFactorySupplier.class);

  @VisibleForTesting
  static class DelegatingSessionFactoryBuilder extends SessionFactoryBuilder {
    private final SessionFactoryBuilder delegate;
    final int sshTimeout;

    DelegatingSessionFactoryBuilder(SessionFactoryBuilder delegate, int sshTimeout) {
      super(null, null, null, -1, null, null);
      this.delegate = delegate;
      this.sshTimeout = sshTimeout;
    }

    @Override
    public SessionFactoryBuilder setConfig(Map<String, String> config) {
      this.config = config;
      return this;
    }

    @Override
    public SessionFactoryBuilder setHostname(String hostname) {
      delegate.setHostname(hostname);
      return this;
    }

    @Override
    public SessionFactoryBuilder setPort(int port) {
      delegate.setPort(port);
      return this;
    }

    @Override
    public SessionFactoryBuilder setProxy(Proxy proxy) {
      delegate.setProxy(proxy);
      return this;
    }

    @Override
    public SessionFactoryBuilder setUsername(String username) {
      delegate.setUsername(username);
      return this;
    }

    @Override
    public SessionFactory build() {
      return new DelegatingSessionFactory(delegate.build(), sshTimeout);
    }
  }

  @VisibleForTesting
  static class DelegatingSessionFactory implements SessionFactory {
    private final SessionFactory delegate;
    final int sshTimeout;

    DelegatingSessionFactory(SessionFactory delegate, int sshTimeout) {
      this.delegate = delegate;
      this.sshTimeout = sshTimeout;
    }

    @Override
    public String getHostname() {
      return delegate.getHostname();
    }

    @Override
    public int getPort() {
      return delegate.getPort();
    }

    @Override
    public Proxy getProxy() {
      return delegate.getProxy();
    }

    @Override
    public String getUsername() {
      return delegate.getUsername();
    }

    @Override
    public Session newSession() throws JSchException {
      Session session = delegate.newSession();
      LOG.info("Setting SSH session timeout to {}", sshTimeout);
      session.setTimeout(sshTimeout);
      return session;
    }

    @Override
    public SessionFactoryBuilder newSessionFactoryBuilder() {
      SessionFactoryBuilder builder = delegate.newSessionFactoryBuilder();
      return new DelegatingSessionFactoryBuilder(builder, sshTimeout);
    }

  }

  private final int sshPort;
  private final String knownHosts;
  private final List<String> identityKeys;
  private final int sshTimeout;
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
