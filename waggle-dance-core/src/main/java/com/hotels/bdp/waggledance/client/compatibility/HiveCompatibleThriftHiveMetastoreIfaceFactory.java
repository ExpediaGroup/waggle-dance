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
package com.hotels.bdp.waggledance.client.compatibility;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TApplicationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;

public class HiveCompatibleThriftHiveMetastoreIfaceFactory {

  private static final Logger log = LoggerFactory.getLogger(HiveCompatibleThriftHiveMetastoreIfaceFactory.class);

  class ThriftMetaStoreClientInvocationHandler implements InvocationHandler {

    private final ThriftHiveMetastore.Client delegate;
    private final HiveThriftMetaStoreIfaceCompatibility compatibility;

    ThriftMetaStoreClientInvocationHandler(
        ThriftHiveMetastore.Client delegate,
        HiveThriftMetaStoreIfaceCompatibility compatibility) {
      this.delegate = delegate;
      this.compatibility = compatibility;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        return method.invoke(delegate, args);
      } catch (InvocationTargetException e) {
        try {
          log.info("Couldn't invoke method {}", method.toGenericString());
          if (e.getCause().getClass().isAssignableFrom(TApplicationException.class)) {
            log.info("Attempting to invoke with {}", compatibility.getClass().getName());
            return invokeCompatibility(method, args);
          }
        } catch (Throwable t) {
          log
              .warn("Unable to run compatibility for metastore client method {}. Will rethrow original exception: ",
                  method.getName(), t);
        }
        throw e.getCause();
      }
    }

    private Object invokeCompatibility(Method method, Object[] args) throws Throwable {
      Class<?>[] argTypes = getTypes(args);
      Method compatibilityMethod = compatibility.getClass().getMethod(method.getName(), argTypes);
      return compatibilityMethod.invoke(compatibility, args);
    }

    private Class<?>[] getTypes(Object[] args) {
      if (args == null) {
        return (Class<?>[]) args;
      }
      Class<?>[] argTypes = new Class<?>[args.length];
      for (int i = 0; i < args.length; ++i) {
        argTypes[i] = args[i].getClass();
      }
      return argTypes;
    }

  }

  public CloseableThriftHiveMetastoreIface newInstance(ThriftHiveMetastore.Client delegate) {
    HiveThriftMetaStoreIfaceCompatibility compatibility = new HiveThriftMetaStoreIfaceCompatibility1xx(delegate);
    return newInstance(delegate, compatibility);
  }

  private CloseableThriftHiveMetastoreIface newInstance(
      ThriftHiveMetastore.Client delegate,
      HiveThriftMetaStoreIfaceCompatibility compatibility) {
    ClassLoader classLoader = CloseableThriftHiveMetastoreIface.class.getClassLoader();
    Class<?>[] interfaces = new Class<?>[] { CloseableThriftHiveMetastoreIface.class };
    ThriftMetaStoreClientInvocationHandler handler = new ThriftMetaStoreClientInvocationHandler(delegate,
        compatibility);
    return (CloseableThriftHiveMetastoreIface) Proxy.newProxyInstance(classLoader, interfaces, handler);
  }

}
