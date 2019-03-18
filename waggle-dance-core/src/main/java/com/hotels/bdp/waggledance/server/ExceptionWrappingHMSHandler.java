/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.waggledance.server.security.NotAllowedException;

public class ExceptionWrappingHMSHandler implements InvocationHandler {

  private final static Logger LOG = LoggerFactory.getLogger(ExceptionWrappingHMSHandler.class);

  private final IHMSHandler baseHandler;

  public static IHMSHandler newProxyInstance(IHMSHandler baseHandler) {
    return (IHMSHandler) Proxy.newProxyInstance(ExceptionWrappingHMSHandler.class.getClassLoader(),
        new Class[] { IHMSHandler.class }, new ExceptionWrappingHMSHandler(baseHandler));
  }

  public ExceptionWrappingHMSHandler(IHMSHandler baseHandler) {
    this.baseHandler = baseHandler;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(baseHandler, args);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof NotAllowedException) {
        // not logging this as this is an "expected" exception, just rewriting it so any client can do something with
        // the thrift exception.
        throw new MetaException("Waggle Dance: " + cause.getMessage());
      } else if (cause instanceof WaggleDanceServerException) {
        LOG.debug("Got error processing '{}' with args '{}'", method, Arrays.toString(args), cause);
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
