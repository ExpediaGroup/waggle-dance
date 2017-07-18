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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Supplier;
import com.pastdev.jsch.SessionFactory;
import com.pastdev.jsch.SessionFactory.SessionFactoryBuilder;
import com.pastdev.jsch.tunnel.Tunnel;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

@RunWith(MockitoJUnitRunner.class)
public class TunnelConnectionManagerFactoryTest {

  public @Mock Supplier<SessionFactory> sessionFactorySupplier;
  public @Mock SessionFactory sessionFactory;
  public @Mock SessionFactoryBuilder sessionFactoryBuilder;

  private TunnelConnectionManagerFactory TunnelConnectionManagerFactory;

  @Before
  public void init() {
    when(sessionFactorySupplier.get()).thenReturn(sessionFactory);
    when(sessionFactory.newSessionFactoryBuilder()).thenReturn(sessionFactoryBuilder);
    TunnelConnectionManagerFactory = new TunnelConnectionManagerFactory(sessionFactorySupplier);
  }

  @Test
  public void remoteDetailsOnly() {
    TunnelConnectionManager tunnelConnectionManager = TunnelConnectionManagerFactory.create("hotels.com", 5678);
    Tunnel tunnel = tunnelConnectionManager.getTunnel("hotels.com", 5678);
    assertThat(tunnel.getAssignedLocalPort() > 0, is(true));
    assertThat(tunnel.getLocalAlias(), is("localhost"));
    assertThat(tunnel.getDestinationHostname(), is("hotels.com"));
    assertThat(tunnel.getDestinationPort(), is(5678));
  }

  @Test
  public void remoteDetailsWithHops() {
    TunnelConnectionManager tunnelConnectionManager = TunnelConnectionManagerFactory.create("a -> b", "hotels.com",
        5678);
    Tunnel tunnel = tunnelConnectionManager.getTunnel("hotels.com", 5678);
    assertThat(tunnel.getAssignedLocalPort() > 0, is(true));
    assertThat(tunnel.getLocalAlias(), is("localhost"));
    assertThat(tunnel.getDestinationHostname(), is("hotels.com"));
    assertThat(tunnel.getDestinationPort(), is(5678));
  }

  @Test
  public void localAndRemoteDetails() {
    TunnelConnectionManager tunnelConnectionManager = TunnelConnectionManagerFactory.create("my-host", 0, "hotels.com",
        5678);
    Tunnel tunnel = tunnelConnectionManager.getTunnel("hotels.com", 5678);
    assertThat(tunnel.getAssignedLocalPort() > 0, is(true));
    assertThat(tunnel.getLocalAlias(), is("my-host"));
    assertThat(tunnel.getDestinationHostname(), is("hotels.com"));
    assertThat(tunnel.getDestinationPort(), is(5678));
  }

  @Test
  public void fullSpec() {
    TunnelConnectionManager tunnelConnectionManager = TunnelConnectionManagerFactory.create("a -> b", "my-host", 1234,
        "hotels.com", 5678);
    Tunnel tunnel = tunnelConnectionManager.getTunnel("hotels.com", 5678);
    assertThat(tunnel.getSpec(), is("my-host:1234:hotels.com:5678"));
  }

  @Test(expected = WaggleDanceException.class)
  public void invalidTunnelExpression() {
    TunnelConnectionManagerFactory.create("@ -> b", "host", 1234, "target", 5678);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidSourceHost() {
    TunnelConnectionManagerFactory.create("a -> b", "host", -1, "target", 5678);
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidTargetHost() {
    TunnelConnectionManagerFactory.create("@ -> b", "host", 1234, "target", -1);
  }

}
