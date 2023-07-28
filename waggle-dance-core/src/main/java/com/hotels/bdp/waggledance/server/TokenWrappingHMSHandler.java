/**
 * Copyright (C) 2016-2023 Expedia, Inc.
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

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.extern.log4j.Log4j2;

@Log4j2
public class TokenWrappingHMSHandler implements InvocationHandler {

  private final IHMSHandler baseHandler;
  private final Boolean useSasl;

  private static final ThreadLocal<String> tokens = new ThreadLocal<String>() {
    @Override
    protected String initialValue() {
      return "";
    }
  };

  public static String getToken() {
    return tokens.get();
  }

  public static void removeToken() {
    tokens.remove();
  }

  public static IHMSHandler newProxyInstance(IHMSHandler baseHandler, boolean useSasl) {
    return (IHMSHandler) Proxy.newProxyInstance(TokenWrappingHMSHandler.class.getClassLoader(),
            new Class[] { IHMSHandler.class }, new TokenWrappingHMSHandler(baseHandler, useSasl));
  }

  public TokenWrappingHMSHandler(IHMSHandler baseHandler, boolean useSasl) {
    this.baseHandler = baseHandler;
    this.useSasl = useSasl;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    try {
      // We will get the token when proxy user call in the first time.
      // Login user must open connect in `TProcessorFactorySaslDecorator#getProcessor`
      // so we can reuse this connect to get proxy user delegation token
      if (useSasl) {
        UserGroupInformation currUser = null;
        String token = null;
        // if call get_delegation_token , will call it directly and set token to threadlocal

        switch (method.getName()) {
          case "get_delegation_token":
            token = (String) method.invoke(baseHandler, args);
            tokens.set(token);
            return token;
          case "close":
            tokens.remove();
            return method.invoke(baseHandler, args);
          default:
            if (tokens.get().isEmpty() && (currUser = UserGroupInformation.getCurrentUser())
                    != UserGroupInformation.getLoginUser()) {

              String shortName = currUser.getShortUserName();
              token = baseHandler.get_delegation_token(shortName, shortName);
              log.info(String.format("get delegation token by user %s", shortName));
              tokens.set(token);
            }
            return method.invoke(baseHandler, args);
        }
      }
      return method.invoke(baseHandler, args);
    } catch (InvocationTargetException e) {
      // Need to unwrap this, so callers get the correct exception thrown by the handler.
      throw e.getCause();
    } catch (UndeclaredThrowableException e) {
      // Need to unwrap this, so callers get the correct exception thrown by the handler.
      throw e.getCause();
    }

  }

}
