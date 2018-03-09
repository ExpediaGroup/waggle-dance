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

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.jcraft.jsch.Proxy;
import com.pastdev.jsch.SessionFactory;
import com.pastdev.jsch.SessionFactory.SessionFactoryBuilder;

@VisibleForTesting
class DelegatingSessionFactoryBuilder extends SessionFactoryBuilder {
  final @VisibleForTesting SessionFactoryBuilder delegate;
  final @VisibleForTesting int sshTimeout;

  DelegatingSessionFactoryBuilder(SessionFactoryBuilder delegate, int sshTimeout) {
    super(null, null, null, -1, null, null);
    this.delegate = delegate;
    this.sshTimeout = sshTimeout;
  }

  @Override
  public SessionFactoryBuilder setConfig(Map<String, String> config) {
    delegate.setConfig(config);
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
