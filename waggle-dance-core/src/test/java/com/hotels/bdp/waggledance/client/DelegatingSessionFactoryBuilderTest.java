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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.jcraft.jsch.Proxy;
import com.pastdev.jsch.SessionFactory;
import com.pastdev.jsch.SessionFactory.SessionFactoryBuilder;

@RunWith(MockitoJUnitRunner.class)
public class DelegatingSessionFactoryBuilderTest {

  private static final int SSH_TIMEOUT = 1000;

  private @Mock SessionFactory delegateSession;
  private @Mock SessionFactoryBuilder delegateBuilder;

  private DelegatingSessionFactoryBuilder builder;

  @Before
  public void init() {
    when(delegateBuilder.build()).thenReturn(delegateSession);
    builder = new DelegatingSessionFactoryBuilder(delegateBuilder, SSH_TIMEOUT);
  }

  @Test
  public void verifyNumberOfMethods() throws Exception {
    assertThat(SessionFactoryBuilder.class.getDeclaredMethods().length, is(6));
  }

  @Test
  public void delegateSetConfig() {
    Map<String, String> config = new HashMap<>();
    assertEquals(builder.setConfig(config), builder);
    verify(delegateBuilder).setConfig(config);
  }

  @Test
  public void delegateSetHostname() {
    String hostname = "hostname";
    assertEquals(builder.setHostname(hostname), builder);
    verify(delegateBuilder).setHostname(hostname);
  }

  @Test
  public void delegateSetPort() {
    int port = 1024;
    assertEquals(builder.setPort(port), builder);
    verify(delegateBuilder).setPort(port);
  }

  @Test
  public void delegateSetProxy() {
    Proxy proxy = mock(Proxy.class);
    assertEquals(builder.setProxy(proxy), builder);
    verify(delegateBuilder).setProxy(proxy);
  }

  @Test
  public void delegateSetUsername() {
    String username = "username";
    assertEquals(builder.setUsername(username), builder);
    verify(delegateBuilder).setUsername(username);
  }

  @Test
  public void delegateBuild() {
    SessionFactory session = builder.build();
    verify(delegateBuilder).build();
    assertThat(session, is(instanceOf(DelegatingSessionFactory.class)));
    assertThat(((DelegatingSessionFactory) session).delegate, is(sameInstance(delegateSession)));
  }

}
