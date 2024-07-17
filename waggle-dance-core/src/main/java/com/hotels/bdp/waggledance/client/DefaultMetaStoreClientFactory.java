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

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.client.compatibility.HiveCompatibleThriftHiveMetastoreIfaceFactory;
import com.hotels.bdp.waggledance.server.TokenWrappingHMSHandler;
import com.hotels.hcommon.hive.metastore.exception.MetastoreUnavailableException;

public class DefaultMetaStoreClientFactory implements MetaStoreClientFactory {

  static final Class<?>[] INTERFACES = new Class<?>[] { CloseableThriftHiveMetastoreIface.class };

  private static class ReconnectingMetastoreClientInvocationHandler implements InvocationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ReconnectingMetastoreClientInvocationHandler.class);

    private final ThriftMetastoreClientManager base;
    private final String name;
    private final int maxRetries;

    private HiveUgiArgs cachedUgi = null;

    private ReconnectingMetastoreClientInvocationHandler(
            String name,
            int maxRetries,
            ThriftMetastoreClientManager base) {
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
            LOG.debug("Error re-opening client at isOpen(): {}", e.getMessage());
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
            LOG
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

    private Object doRealCall(Method method, Object[] args, int attempt) throws IllegalAccessException, Throwable {
      do {
        try {
          return method.invoke(base.getClient(), args);
        } catch (InvocationTargetException e) {
          Throwable realException = e.getTargetException();
          if (TTransportException.class.isAssignableFrom(realException.getClass())) {
            if (attempt < maxRetries && shouldRetry(method)) {
              LOG.debug("TTransportException captured in client {}. Reconnecting... ", name);
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

  private static class SaslMetastoreClientHandler implements InvocationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SaslMetastoreClientHandler.class);

    private final CloseableThriftHiveMetastoreIface baseHandler;
    private final ThriftMetastoreClientManager clientManager;
    private final String tokenSignature = "WAGGLEDANCETOKEN";

    private String delegationToken;

    public static CloseableThriftHiveMetastoreIface newProxyInstance(
            CloseableThriftHiveMetastoreIface baseHandler,
            ThriftMetastoreClientManager clientManager) {
      return (CloseableThriftHiveMetastoreIface) Proxy.newProxyInstance(SaslMetastoreClientHandler.class.getClassLoader(),
              INTERFACES, new SaslMetastoreClientHandler(baseHandler, clientManager));
    }

    private SaslMetastoreClientHandler(
            CloseableThriftHiveMetastoreIface handler,
            ThriftMetastoreClientManager clientManager) {
      this.baseHandler = handler;
      this.clientManager = clientManager;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      try {
        switch (method.getName()) {
          case "get_delegation_token":
            try {
              clientManager.open();
              Object token = method.invoke(baseHandler, args);
              this.delegationToken = (String) token;
              clientManager.close();
              setTokenStr2Ugi(UserGroupInformation.getCurrentUser(), (String) token);
              clientManager.open();
              return token;
            } catch (IOException e) {
              throw new MetastoreUnavailableException("Couldn't setup delegation token in the ugi: ", e);
            }
          default:
            genToken();
            return method.invoke(baseHandler, args);
        }
      } catch (InvocationTargetException e) {
        throw e.getTargetException();
      } catch (UndeclaredThrowableException e) {
        throw e.getCause();
      }
    }

    private void genToken() throws Throwable {
      UserGroupInformation currUser = null;
      if (delegationToken == null && (currUser = UserGroupInformation.getCurrentUser())
              != UserGroupInformation.getLoginUser()) {

        LOG.info(String.format("set %s delegation token", currUser.getShortUserName()));
        String token = TokenWrappingHMSHandler.getToken();
        setTokenStr2Ugi(currUser, token);
        delegationToken = token;
        clientManager.close();
      }
    }

    private void setTokenStr2Ugi(UserGroupInformation currUser, String token) throws IOException {
      String newTokenSignature = clientManager.generateNewTokenSignature(tokenSignature);
      Utils.setTokenStr(currUser, token, newTokenSignature);
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
    return newInstance(name, reconnectionRetries, new ThriftMetastoreClientManager(hiveConf,
        new HiveCompatibleThriftHiveMetastoreIfaceFactory(), connectionTimeout));
  }

  @VisibleForTesting
  CloseableThriftHiveMetastoreIface newInstance(
      String name,
      int reconnectionRetries,
      ThriftMetastoreClientManager base) {
    ReconnectingMetastoreClientInvocationHandler reconnectingHandler = new ReconnectingMetastoreClientInvocationHandler(
            name, reconnectionRetries, base);
    if (base.isSaslEnabled()) {
      CloseableThriftHiveMetastoreIface ifaceReconnectingHandler = (CloseableThriftHiveMetastoreIface) Proxy
              .newProxyInstance(getClass().getClassLoader(), INTERFACES, reconnectingHandler);
      // wrapping the SaslMetastoreClientHandler to handle delegation token if using sasl
      return SaslMetastoreClientHandler.newProxyInstance(ifaceReconnectingHandler, base);
    } else {
      return (CloseableThriftHiveMetastoreIface) Proxy
              .newProxyInstance(getClass().getClassLoader(), INTERFACES, reconnectingHandler);
    }

  }

}
