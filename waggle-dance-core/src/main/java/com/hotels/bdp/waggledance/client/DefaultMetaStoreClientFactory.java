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
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.thrift.transport.TTransportException;

import lombok.extern.log4j.Log4j2;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.client.compatibility.HiveCompatibleThriftHiveMetastoreIfaceFactory;
import com.hotels.hcommon.hive.metastore.exception.MetastoreUnavailableException;


public class DefaultMetaStoreClientFactory implements MetaStoreClientFactory {

  static final Class<?>[] INTERFACES = new Class<?>[] { CloseableThriftHiveMetastoreIface.class };

  @Log4j2
  private static class ReconnectingMetastoreClientInvocationHandler implements InvocationHandler {

    private final AbstractThriftMetastoreClientManager base;
    private final String name;
    private final int maxRetries;

    private HiveUgiArgs cachedUgi = null;

    private ReconnectingMetastoreClientInvocationHandler(
            String name,
            int maxRetries,
        AbstractThriftMetastoreClientManager base) {
      this.name = name;
      this.maxRetries = maxRetries;
      this.base = base;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      int attempt = 0;
      // close() and isOpen() methods delegate to base HiveMetastoreClient
      switch (method.getName()) {
        case "isOpen":
          try {
            reconnectIfDisconnected();
            return base.isOpen();
          } catch (Exception e) {
            log.debug("Error re-opening client at isOpen(): {}", e.getMessage());
            return false;
          }
        case "close":
          if (base != null) {
            base.close();
          }
          return null;
        case "set_ugi":
          String user = (String) args[0];
          List<String> groups = (List<String>) args[1];
          cachedUgi = new HiveUgiArgs(user, groups);
          if (base.isOpen()) {
            log
                    .info("calling #set_ugi (on already open client) for user '{}',  on metastore {}", cachedUgi.getUser(),
                            name);
            return doRealCall(method, args, attempt);
          } else {
            // delay call until we get the next non set_ugi call, this helps doing unnecessary calls to Federated
            // Metastores.
            return Lists.newArrayList(user);
          }
        default:
          base.open(cachedUgi);
          return doRealCall(method, args, attempt);
      }
    }

    private Object doRealCall(Method method, Object[] args, int attempt) throws Throwable {
      do {
        try {
          return method.invoke(base.getClient(), args);
        } catch (InvocationTargetException e) {
          Throwable realException = e.getTargetException();
          if (TTransportException.class.isAssignableFrom(realException.getClass())) {
            if (attempt < maxRetries && shouldRetry(method)) {
              log.debug("TTransportException captured in client {}. Reconnecting... ", name);
              base.reconnect(cachedUgi);
              continue;
            }
            throw new MetastoreUnavailableException("Client " + name + " is not available", realException);
          }
          throw realException;
        }
      } while (++attempt <= maxRetries);
      throw new RuntimeException("Unreachable code");
    }

    private boolean shouldRetry(Method method) {
      switch (method.getName()) {
        case "shutdown":
          return false;
        default:
          return true;
      }
    }

    private void reconnectIfDisconnected() {
      try {
        if (!base.isOpen()) {
          base.reconnect(cachedUgi);
        }
      } catch (Exception e) {
        throw new MetastoreUnavailableException("Client " + name + " is not available", e);
      }
    }

  }

  /*
   * (non-Javadoc)
   * @see com.hotels.bdp.waggledance.client.MetaStoreClientFactoryI#newInstance(org.apache.hadoop.hive.conf.HiveConf,
   * java.lang.String, int)
   */
  @Override
  public CloseableThriftHiveMetastoreIface newInstance(
          HiveConf hiveConf,
          String name,
          int reconnectionRetries,
          int connectionTimeout) {
    boolean useSasl = hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL);
    HiveCompatibleThriftHiveMetastoreIfaceFactory factory = new HiveCompatibleThriftHiveMetastoreIfaceFactory();
    AbstractThriftMetastoreClientManager base = null;
    if (useSasl) {
      base = new SaslThriftMetastoreClientManager(hiveConf, factory, connectionTimeout);
    } else {
      base = new ThriftMetastoreClientManager(hiveConf, factory, connectionTimeout);
    }
    return newInstance(name, reconnectionRetries, base);
  }

  @VisibleForTesting
  CloseableThriftHiveMetastoreIface newInstance(
          String name,
          int reconnectionRetries,
          AbstractThriftMetastoreClientManager base) {
    ReconnectingMetastoreClientInvocationHandler reconnectingHandler = new ReconnectingMetastoreClientInvocationHandler(
        name, reconnectionRetries, base);
    return (CloseableThriftHiveMetastoreIface) Proxy.newProxyInstance(getClass().getClassLoader(),
        INTERFACES, reconnectingHandler);
  }

}