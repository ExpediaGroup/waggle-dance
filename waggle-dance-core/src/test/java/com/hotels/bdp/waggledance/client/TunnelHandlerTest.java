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

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.jcraft.jsch.JSchException;
import com.pastdev.jsch.tunnel.Tunnel;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

@RunWith(MockitoJUnitRunner.class)
public class TunnelHandlerTest {
  private @Mock TunnelConnectionManager tunnelConnectionManager;
  private @Mock Tunnel tunnel;

  private TunnelHandler tunnelHandler = new TunnelHandler();

  @Before
  public void init() {
    when(tunnelConnectionManager.getTunnel(anyString(), anyInt())).thenReturn(tunnel);
  }

  @Test
  public void openTunnel() throws JSchException {
    tunnelHandler.openTunnel(tunnelConnectionManager, "route", "localhost", "remotehost", 1234);
    verify(tunnelConnectionManager, times(1)).getTunnel(anyString(), anyInt());
    verify(tunnelConnectionManager).open();
  }

  @Test(expected = WaggleDanceException.class)
  public void openThrowsJSchException() throws JSchException {
    doThrow(new JSchException()).when(tunnelConnectionManager).open();
    tunnelHandler.openTunnel(tunnelConnectionManager, "route", "localhost", "remotehost", 1234);
  }

  @Test(expected = WaggleDanceException.class)
  public void openThrowsRuntimeException() throws JSchException {
    doThrow(new RuntimeException()).when(tunnelConnectionManager).open();
    tunnelHandler.openTunnel(tunnelConnectionManager, "route", "localhost", "remotehost", 1234);
  }

}
