/**
 * Copyright (C) 2016-2024 Expedia, Inc.
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * This class splits the traffic for read only calls (get* for instance getTable, getPartition) to the readOnly client
 * and everything else will go to readWrite client.
 */
public class SplitTrafficMetastoreClientFactory {

  static final Class<?>[] INTERFACES = new Class<?>[] { CloseableThriftHiveMetastoreIface.class };

  private static class SplitTrafficClientInvocationHandler implements InvocationHandler {

    private final CloseableThriftHiveMetastoreIface readWrite;
    private final CloseableThriftHiveMetastoreIface readOnly;

    public SplitTrafficClientInvocationHandler(
        CloseableThriftHiveMetastoreIface readWrite,
        CloseableThriftHiveMetastoreIface readOnly) {
      this.readWrite = readWrite;
      this.readOnly = readOnly;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      switch (method.getName()) {
      case "isOpen":
        return readWrite.isOpen() && readOnly.isOpen();
      case "close":
        try {
          readWrite.close();
        } finally {
          readOnly.close();
        }
        return null;
      case "set_ugi":
        Object result = doRealCall(readWrite, method, args);
        // we skip the result for readOnly (it should always be the same).
        doRealCall(readOnly, method, args);
        return result;
      default:
        if (method.getName().startsWith("get")) {
          return doRealCall(readOnly, method, args);
        }
        return doRealCall(readWrite, method, args);
      }
    }

    private Object doRealCall(CloseableThriftHiveMetastoreIface client, Method method, Object[] args)
      throws IllegalAccessException, Throwable {
      try {
        return method.invoke(client, args);
      } catch (InvocationTargetException e) {
        Throwable realException = e.getTargetException();
        throw realException;
      }
    }
  }

  public CloseableThriftHiveMetastoreIface newInstance(
      CloseableThriftHiveMetastoreIface readWrite,
      CloseableThriftHiveMetastoreIface readOnly) {
    return (CloseableThriftHiveMetastoreIface) Proxy
        .newProxyInstance(getClass().getClassLoader(), INTERFACES,
            new SplitTrafficClientInvocationHandler(readWrite, readOnly));
  }

}
