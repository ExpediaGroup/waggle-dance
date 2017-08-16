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

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.jcraft.jsch.JSchException;
import com.pastdev.jsch.DefaultSessionFactory;
import com.pastdev.jsch.SessionFactory;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

public class SessionFactorySupplier implements Supplier<SessionFactory> {
  private static final Logger LOG = LoggerFactory.getLogger(SessionFactorySupplier.class);

  private final int sshPort;
  private final String knownHosts;
  private final List<String> identityKeys;
  private DefaultSessionFactory sessionFactory = null;

  public SessionFactorySupplier(int sshPort, String knownHosts, List<String> identityKeys) {
    this.sshPort = sshPort;
    this.knownHosts = knownHosts;
    this.identityKeys = ImmutableList.copyOf(identityKeys);
  }

  @Override
  public SessionFactory get() {
    if (sessionFactory == null) {
      try {
        synchronized (this) {
          System.setProperty(DefaultSessionFactory.PROPERTY_JSCH_KNOWN_HOSTS_FILE, knownHosts);
          sessionFactory = new DefaultSessionFactory();
          sessionFactory.setPort(sshPort);
          LOG.debug("Session factory created for {}@{}:{}", sessionFactory.getUsername(), sessionFactory.getHostname(),
              sessionFactory.getPort());
        }
        sessionFactory.setIdentitiesFromPrivateKeys(identityKeys);
      } catch (JSchException | RuntimeException e) {
        throw new WaggleDanceException(
            "Unable to create factory with knownHosts=" + knownHosts + " and identityKeys=" + identityKeys, e);
      }
    }
    return sessionFactory;
  }

}
