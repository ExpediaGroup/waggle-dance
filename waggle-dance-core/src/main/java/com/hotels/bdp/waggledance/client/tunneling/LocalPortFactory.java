package com.hotels.bdp.waggledance.client.tunneling;

import java.io.IOException;
import java.net.ServerSocket;

import com.hotels.hcommon.ssh.SshException;

class LocalPortFactory {

  int getLocalPort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      return socket.getLocalPort();
    } catch (IOException | RuntimeException e) {
      throw new SshException("Unable to bind to a free localhost port", e);
    }
  }

}
