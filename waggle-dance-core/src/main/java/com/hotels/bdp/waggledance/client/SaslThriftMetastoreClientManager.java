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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.service.auth.KerberosSaslHelper;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import lombok.extern.log4j.Log4j2;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.hotels.bdp.waggledance.client.compatibility.HiveCompatibleThriftHiveMetastoreIfaceFactory;
import com.hotels.bdp.waggledance.context.CommonBeans;

@Log4j2
public class SaslThriftMetastoreClientManager extends AbstractThriftMetastoreClientManager {

  private final boolean impersonationEnabled;
  private static final Duration delegationTokenCacheTtl = Duration.ofHours(
      1); // The default lifetime in Hive is 7 days (metastore.cluster.delegation.token.max-lifetime)
  private static final long delegationTokenCacheMaximumSize = 1000;
  private static final LoadingCache<DelegationTokenKey, String> delegationTokenCache = CacheBuilder.newBuilder()
      .expireAfterWrite(delegationTokenCacheTtl.toMillis(), MILLISECONDS)
      .maximumSize(delegationTokenCacheMaximumSize)
      .build(CacheLoader.from(SaslThriftMetastoreClientManager::loadDelegationToken));

  SaslThriftMetastoreClientManager(HiveConf conf,
      HiveCompatibleThriftHiveMetastoreIfaceFactory hiveCompatibleThriftHiveMetastoreIfaceFactory,
      int connectionTimeout) {
    super(conf, hiveCompatibleThriftHiveMetastoreIfaceFactory, connectionTimeout);
    impersonationEnabled = conf.getBoolean(CommonBeans.IMPERSONATION_ENABLED_KEY, false);
  }

  @Override
  void open(HiveUgiArgs ugiArgs) {
    if (isConnected) {
      return;
    }
    createMetastoreClientAndOpen(null, ugiArgs);
    if (impersonationEnabled) {
      try {
        String userName = UserGroupInformation.getCurrentUser().getShortUserName();
        DelegationTokenKey key = new DelegationTokenKey(msUri, userName, client);
        String delegationToken = delegationTokenCache.get(key);
        close();
        createMetastoreClientAndOpen(delegationToken, ugiArgs);
      } catch (IOException | ExecutionException e) {
        log.error("Couldn't create delegation token client");
        throw new RuntimeException(e);
      }
    }
  }

  private void createMetastoreClientAndOpen(String delegationToken, HiveUgiArgs ugiArgs) {
    TException te = null;
    boolean useSsl = conf.getBoolVar(ConfVars.HIVE_METASTORE_USE_SSL);
    boolean useCompactProtocol = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL);
    int clientSocketTimeout = (int) conf.getTimeVar(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT,
        TimeUnit.MILLISECONDS);

    for (int attempt = 0; !isConnected && (attempt < retries); ++attempt) {
      for (URI store : metastoreUris) {
        log.info("Trying to connect to metastore with URI {}", store);
        try {
          transport = new TSocket(store.getHost(), store.getPort(), clientSocketTimeout,
              connectionTimeout);
          // Wrap thrift connection with SASL for secure connection.
          try {
            UserGroupInformation.setConfiguration(conf);

            // check if we should use delegation tokens to authenticate
            // the call below gets hold of the tokens if they are set up by hadoop
            // this should happen on the map/reduce tasks if the client added the
            // tokens into hadoop's credential store in the front end during job
            // submission.
            if (impersonationEnabled && delegationToken != null) {
              // authenticate using delegation tokens via the "DIGEST" mechanism
              transport = KerberosSaslHelper
                  .getTokenTransport(delegationToken,
                      store.getHost(), transport,
                      MetaStoreUtils.getMetaStoreSaslProperties(conf, useSsl));
            } else {
              String principalConfig = conf.getVar(ConfVars.METASTORE_KERBEROS_PRINCIPAL);
              transport = UserGroupInformation.getLoginUser().doAs(
                  (PrivilegedExceptionAction<TTransport>) () -> KerberosSaslHelper.getKerberosTransport(
                      principalConfig, store.getHost(), transport,
                      MetaStoreUtils.getMetaStoreSaslProperties(conf, useSsl), false));
            }
          } catch (IOException | InterruptedException exception) {
            log.error("Couldn't create client transport, URI " + store, exception);
            throw new MetaException(exception.toString());
          }

          TProtocol protocol;
          if (useCompactProtocol) {
            protocol = new TCompactProtocol(transport);
          } else {
            protocol = new TBinaryProtocol(transport);
          }
          client = hiveCompatibleThriftHiveMetastoreIfaceFactory.newInstance(
              new ThriftHiveMetastore.Client(protocol));
          try {
            transport.open();
            log
                .info("Opened a connection to metastore '"
                    + store
                    + "', total current connections to all metastores: "
                    + CONN_COUNT.incrementAndGet());

            isConnected = true;
            if (ugiArgs != null) {
              log.info("calling #set_ugi for user '{}',  on URI {}", ugiArgs.getUser(), store);
              client.set_ugi(ugiArgs.getUser(), ugiArgs.getGroups());
            } else {
              log.debug("Connection opened with out #set_ugi call',  on URI {}", store);
            }
          } catch (TException e) {
            te = e;
            if (log.isDebugEnabled()) {
              log.warn("Failed to connect to the MetaStore Server, URI " + store, e);
            } else {
              // Don't print full exception trace if DEBUG is not on.
              log.warn("Failed to connect to the MetaStore Server, URI {}", store);
            }
          }
        } catch (MetaException e) {
          log.error("Unable to connect to metastore with URI " + store + " in attempt " + attempt,
              e);
        }
        if (isConnected) {
          break;
        }
      }
      // Wait before launching the next round of connection retries.
      if (!isConnected && (retryDelaySeconds > 0) && ((attempt + 1) < retries)) {
        try {
          log.info("Waiting {} seconds before next connection attempt.", retryDelaySeconds);
          Thread.sleep(retryDelaySeconds * 1000);
        } catch (InterruptedException ignore) {
        }
      }
    }

    if (!isConnected) {
      throw new RuntimeException("Could not connect to meta store using any of the URIs ["
          + msUri
          + "] provided. Most recent failure: "
          + StringUtils.stringifyException(te));
    }
    log.debug("Connected to metastore.");
  }

  private static class DelegationTokenKey {

    String msUri;
    String username;
    ThriftHiveMetastore.Iface client;

    public DelegationTokenKey(String msUri, String username, Iface client) {
      this.msUri = msUri;
      this.username = username;
      this.client = client;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      DelegationTokenKey that = (DelegationTokenKey) o;
      return Objects.equals(msUri, that.msUri) && Objects.equals(username,
          that.username);
    }

    @Override
    public int hashCode() {
      return Objects.hash(msUri, username);
    }

  }

  private static String loadDelegationToken(DelegationTokenKey key) {
    try {
      return key.client.get_delegation_token(key.username, key.username);
    } catch (TException e) {
      log.error("could not get delegation token,username:{},uri: {}", key.username, key.msUri);
      throw new RuntimeException(e);
    }
  }
}
