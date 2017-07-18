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
package com.hotels.bdp.waggledance.junit;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.net.ConnectException;
import java.net.Socket;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class ServerSocketRuleTest {

  private final ServerSocketRule rule = new ServerSocketRule();

  private void sendData(int port, byte[] bytes) throws Exception {
    try (Socket socket = new Socket("localhost", port)) {
      socket.getOutputStream().write(bytes);
    }
  }

  @Test
  public void typical() throws Throwable {
    rule.before();
    sendData(rule.port(), "my-data".getBytes());
    rule.awaitRequests(1, 1, TimeUnit.SECONDS);
    assertThat(new String(rule.getOutput()), is("my-data"));
    rule.after();
  }

  @Test
  public void notInitialised() throws Throwable {
    sendData(rule.port(), "my-data".getBytes());
    assertThat(new String(rule.getOutput()), is(""));
  }

  @Test(expected = ConnectException.class)
  public void alreadyShutdown() throws Throwable {
    rule.before();
    rule.after();

    sendData(rule.port(), "my-data".getBytes());
  }

  @Test(expected = RuntimeException.class)
  public void noDataSent() throws Throwable {
    rule.before();
    try {
      rule.awaitRequests(100, 1, TimeUnit.SECONDS);
    } finally {
      rule.after();
    }
  }

  @Test
  public void expectMultipleRequests() throws Throwable {
    rule.before();
    sendData(rule.port(), "1".getBytes());
    sendData(rule.port(), "2".getBytes());
    rule.awaitRequests(2, 1, TimeUnit.SECONDS);
    assertThat(new String(rule.getOutput()), is("12"));
    rule.after();
  }

}
