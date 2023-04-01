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
package com.hotels.bdp.waggledance.client;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.client.compatibility.HiveCompatibleThriftHiveMetastoreIfaceFactory;
import com.hotels.hcommon.hive.metastore.exception.MetastoreUnavailableException;

public class DefaultMetaStoreClientFactory implements MetaStoreClientFactory {

  static final Class<?>[] INTERFACES = new Class<?>[] { CloseableThriftHiveMetastoreIface.class };

  private static class ReconnectingMetastoreClientInvocationHandler implements InvocationHandler {
    private static final Logger LOG = LoggerFactory.getLogger(ReconnectingMetastoreClientInvocationHandler.class);

    private final ThriftMetastoreClientManager base;
    private final String name;
    private final int maxRetries;
    private final String tokenSignature = "WAGGLEDANCETOKEN";
    private final boolean useSasl;

    private String delegationToken;
    private HiveUgiArgs cachedUgi = null;

    private ReconnectingMetastoreClientInvocationHandler(
        String name,
        int maxRetries,
        ThriftMetastoreClientManager base) {
      this.name = name;
      this.maxRetries = maxRetries;
      this.base = base;
      this.useSasl = base.getHiveConfBool(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL);
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
      case "get_delegation_token":
        try {
          base.open(cachedUgi);
          Object token = doRealCall(method, args, attempt);
          if (useSasl) {
            this.delegationToken = (String) token;
            base.close();
            String newTokenSignature = base.getHiveConfValue(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE.varname,
                    tokenSignature);
            base.setHiveConfValue(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE.varname,
                    newTokenSignature);
            Utils.setTokenStr(UserGroupInformation.getCurrentUser(), (String) token, newTokenSignature);
            base.open(cachedUgi);
          }
          return token;
        } catch (IOException | TException e) {
          throw new MetastoreUnavailableException("Couldn't setup delegation token in the ugi: ", e);
        }
      default:
        genDelegationToken();
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

    private String genDelegationToken() throws Throwable {
      base.open(cachedUgi);

      UserGroupInformation currUser = UserGroupInformation.getCurrentUser();
      if (delegationToken == null && currUser != UserGroupInformation.getLoginUser() && useSasl) {
        String currShortName = currUser.getShortUserName();
        Method getTokenMethod =
                ThriftHiveMetastore.Iface.class.getMethod("get_delegation_token",
                        String.class, String.class);
        Object[] args = new Object[2];
        args[0] = currShortName;
        args[1] = currShortName;
        String token = (String) doRealCall(getTokenMethod, args, 0);
        base.close();
        String newTokenSignature = base.getHiveConfValue(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE.varname,
                tokenSignature);
        base.setHiveConfValue(HiveConf.ConfVars.METASTORE_TOKEN_SIGNATURE.varname,
                newTokenSignature);
        Utils.setTokenStr(currUser, token, newTokenSignature);
        this.delegationToken = token;
      }
      if (!base.isOpen()) {
        base.open(cachedUgi);
      }
      return delegationToken;
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
    return (CloseableThriftHiveMetastoreIface) Proxy
        .newProxyInstance(getClass().getClassLoader(), INTERFACES, reconnectingHandler);
  }

}
