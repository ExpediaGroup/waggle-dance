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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Proxy;
import com.jcraft.jsch.Session;
import com.pastdev.jsch.SessionFactory;
import com.pastdev.jsch.SessionFactory.SessionFactoryBuilder;

@RunWith(MockitoJUnitRunner.class)
public class DelegatingSessionFactoryTest {

  private static final int SSH_TIMEOUT = 1000;

  private @Mock SessionFactory delegate;

  private DelegatingSessionFactory factory;

  @Before
  public void init() throws Exception {
    factory = new DelegatingSessionFactory(delegate, SSH_TIMEOUT);
  }

  @Test
  public void verifyNumberOfMethods() throws Exception {
    assertThat(SessionFactory.class.getDeclaredMethods().length, is(6));
  }

  @Test
  public void delegateGetHostname() {
    when(delegate.getHostname()).thenReturn("hostname");
    assertThat(factory.getHostname(), is("hostname"));
    verify(delegate).getHostname();
  }

  @Test
  public void delegateGetPort() {
    when(delegate.getPort()).thenReturn(1024);
    assertThat(factory.getPort(), is(1024));
    verify(delegate).getPort();
  }

  @Test
  public void delegateGetProxy() {
    Proxy proxy = mock(Proxy.class);
    when(delegate.getProxy()).thenReturn(proxy);
    assertThat(factory.getProxy(), is(sameInstance(proxy)));
    verify(delegate).getProxy();
  }

  @Test
  public void delegateGetUsername() {
    when(delegate.getUsername()).thenReturn("username");
    assertThat(factory.getUsername(), is("username"));
    verify(delegate).getUsername();
  }

  @Test
  public void delegateNewSession() throws JSchException {
    Session session = mock(Session.class);
    when(delegate.newSession()).thenReturn(session);
    assertThat(factory.newSession(), is(sameInstance(session)));
    verify(delegate).newSession();
    verify(session).setTimeout(SSH_TIMEOUT);
  }

  @Test
  public void delegateNewSessionFactoryBuilder() {
    SessionFactoryBuilder builder = mock(SessionFactoryBuilder.class);
    when(delegate.newSessionFactoryBuilder()).thenReturn(builder);
    SessionFactoryBuilder delegateBuilder = factory.newSessionFactoryBuilder();
    assertThat(delegateBuilder, is(instanceOf(DelegatingSessionFactoryBuilder.class)));
    assertThat(((DelegatingSessionFactoryBuilder) delegateBuilder).delegate, is(sameInstance(builder)));
    verify(delegate).newSessionFactoryBuilder();
  }

}
