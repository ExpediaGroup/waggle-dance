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
package com.hotels.bdp.waggledance.client.compatibility;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TApplicationException;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;

@Log4j2
public class HiveCompatibleThriftHiveMetastoreIfaceFactory {

  @AllArgsConstructor
  private static class ThriftMetaStoreClientInvocationHandler implements InvocationHandler {

    private final ThriftHiveMetastore.Client delegate;
    private final HiveThriftMetaStoreIfaceCompatibility1xx compatibility;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(delegate, args);
      } catch (InvocationTargetException delegateException) {
        try {
          log.info("Couldn't invoke method {}", method.toGenericString());
          if (delegateException.getCause().getClass().isAssignableFrom(TApplicationException.class)) {
            log.info("Attempting to invoke with {}", compatibility.getClass().getName());
            return invokeCompatibility(method, args);
          }
        } catch (InvocationTargetException compatibilityException) {
          if (compatibilityException.getCause().getClass().isAssignableFrom(TApplicationException.class)) {
            log
                .warn(
                    "Invocation of compatibility for metastore client method {} failed. Will rethrow original exception, logging exception from compatibility layer",
                    method.getName(), compatibilityException);
          } else {
            throw compatibilityException.getCause();
          }
        } catch (NoSuchMethodException e) {
          log
              .debug(
                  "Compatibility layer has no such method '" + method.getName() + "'. Will rethrow original exception",
                  e);
        } catch (Throwable t) {
          log
              .warn("Unable to invoke compatibility for metastore client method "
                  + method.getName()
                  + ". Will rethrow original exception, logging exception from invocation handler", t);
        }
        throw delegateException.getCause();
      }
    }

    private Object invokeCompatibility(Method method, Object[] args) throws Throwable {
      Method compatibilityMethod = compatibility.getClass().getMethod(method.getName(), method.getParameterTypes());
      return compatibilityMethod.invoke(compatibility, args);
    }

  }

  public CloseableThriftHiveMetastoreIface newInstance(ThriftHiveMetastore.Client delegate) {
    HiveThriftMetaStoreIfaceCompatiblity compatibility = new HMSCompatiblityImpl(delegate);
    return newInstance(delegate, compatibility);
  }

  private CloseableThriftHiveMetastoreIface newInstance(
      ThriftHiveMetastore.Client delegate,
      HiveThriftMetaStoreIfaceCompatiblity compatibility) {
    ClassLoader classLoader = CloseableThriftHiveMetastoreIface.class.getClassLoader();
    Class<?>[] interfaces = new Class<?>[] { CloseableThriftHiveMetastoreIface.class };
    ThriftMetaStoreClientInvocationHandler handler = new ThriftMetaStoreClientInvocationHandler(delegate,
        compatibility);
    return (CloseableThriftHiveMetastoreIface) Proxy.newProxyInstance(classLoader, interfaces, handler);
  }

}
