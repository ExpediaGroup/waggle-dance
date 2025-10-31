/**
 * Copyright (C) 2016-2025 Expedia, Inc.
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
package com.hotels.bdp.waggledance.server;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.waggledance.server.security.NotAllowedException;

public class ExceptionWrappingHMSHandler implements InvocationHandler {
  private static Logger log = LoggerFactory.getLogger(ExceptionWrappingHMSHandler.class);

  private final FederatedHMSHandlerFactory federatedHMSHandlerFactory;
  private CloseableIHMSHandler baseHandler;
  private String user = "";


  public static CloseableIHMSHandler newProxyInstance(FederatedHMSHandlerFactory federatedHMSHandlerFactory) {
    return (CloseableIHMSHandler) Proxy
        .newProxyInstance(ExceptionWrappingHMSHandler.class.getClassLoader(),
            new Class[] { CloseableIHMSHandler.class }, new ExceptionWrappingHMSHandler(federatedHMSHandlerFactory));
  }

  public ExceptionWrappingHMSHandler(FederatedHMSHandlerFactory federatedHMSHandlerFactory) {
    this.baseHandler = null;
    this.federatedHMSHandlerFactory = federatedHMSHandlerFactory;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if (baseHandler == null) {
      baseHandler = federatedHMSHandlerFactory.create();
    }
    if (method.getName().equals("set_ugi")) {
      user = (String) args[0];
    }
    try {
      log
          .info("WD Audit:[User:{}, method:{}, args:{}]", user, method.getName(),
              StringUtils.left(Arrays.toString(args), 256));
      return method.invoke(baseHandler, args);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof NotAllowedException) {
        // not logging this as this is an "expected" exception, just rewriting it so any client can do something with
        // the thrift exception.
        throw new MetaException("Waggle Dance: " + cause.getMessage());
      } else if (cause instanceof WaggleDanceServerException) {
        log.debug("Got error processing '{}' with args '{}'", method, Arrays.toString(args), cause);
        throw new MetaException("Waggle Dance: " + cause.getMessage());
      } else {
        // Need to unwrap this, so callers get the correct exception thrown by the handler.
        throw e.getCause();
      }
    } catch (UndeclaredThrowableException e) {
      // Need to unwrap this, so callers get the correct exception thrown by the handler.
      throw e.getCause();
    }
  }

}
