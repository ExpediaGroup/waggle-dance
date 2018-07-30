package com.hotels.bdp.waggledance.client.tunneling;

import java.lang.reflect.Method;

import com.hotels.hcommon.ssh.MethodChecker;

public class MetastoreClientMethodChecker implements MethodChecker {
  @Override
  public boolean isTunnelled(Method method) {
    return "open".equals(method.getName()) || "reconnect".equals(method.getName());
  }

  @Override
  public boolean isShutdown(Method method) {
    return "close".equals(method.getName());
  }
}
