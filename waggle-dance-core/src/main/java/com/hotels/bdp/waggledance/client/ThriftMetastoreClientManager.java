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

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfUtil;
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
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import lombok.extern.log4j.Log4j2;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import com.hotels.bdp.waggledance.client.compatibility.HiveCompatibleThriftHiveMetastoreIfaceFactory;
import com.hotels.bdp.waggledance.context.CommonBeans;

@Log4j2
class ThriftMetastoreClientManager implements Closeable {


  private static final AtomicInteger CONN_COUNT = new AtomicInteger(0);
  private final HiveConf conf;
  private final HiveCompatibleThriftHiveMetastoreIfaceFactory hiveCompatibleThriftHiveMetastoreIfaceFactory;
  private final URI[] metastoreUris;
  private ThriftHiveMetastore.Iface client = null;
  private TTransport transport = null;
  private boolean isConnected = false;
  // for thrift connects
  private int retries = 5;
  private long retryDelaySeconds = 0;

  private final int connectionTimeout;
  private final String msUri;
  private final boolean impersonationEnabled;
  private static final Duration delegationTokenCacheTtl = Duration.ofHours(1); // The default lifetime in Hive is 7 days (metastore.cluster.delegation.token.max-lifetime)
  private static final long delegationTokenCacheMaximumSize = 1000;
  private static final LoadingCache<DelegationTokenKey, String> delegationTokenCache = CacheBuilder.newBuilder()
      .expireAfterWrite(delegationTokenCacheTtl.toMillis(), MILLISECONDS)
      .maximumSize(delegationTokenCacheMaximumSize)
      .build(CacheLoader.from(ThriftMetastoreClientManager::loadDelegationToken));

  ThriftMetastoreClientManager(
      HiveConf conf,
      HiveCompatibleThriftHiveMetastoreIfaceFactory hiveCompatibleThriftHiveMetastoreIfaceFactory,
      int connectionTimeout) {
    this.conf = conf;
    this.hiveCompatibleThriftHiveMetastoreIfaceFactory = hiveCompatibleThriftHiveMetastoreIfaceFactory;
    this.connectionTimeout = connectionTimeout;
    msUri = conf.getVar(ConfVars.METASTOREURIS);
    impersonationEnabled = conf.getBoolean(CommonBeans.IMPERSONATION_ENABLED_KEY,false);

    if (HiveConfUtil.isEmbeddedMetaStore(msUri)) {
      throw new RuntimeException("You can't waggle an embedded metastore");
    }

    // get the number retries
    retries = HiveConf.getIntVar(conf, ConfVars.METASTORETHRIFTCONNECTIONRETRIES);
    retryDelaySeconds = conf.getTimeVar(ConfVars.METASTORE_CLIENT_CONNECT_RETRY_DELAY, TimeUnit.SECONDS);

    // user wants file store based configuration
    if (msUri != null) {
      String[] metastoreUrisString = msUri.split(",");
      metastoreUris = new URI[metastoreUrisString.length];
      try {
        int i = 0;
        for (String s : metastoreUrisString) {
          URI tmpUri = new URI(s);
          if (tmpUri.getScheme() == null) {
            throw new IllegalArgumentException("URI: " + s + " does not have a scheme");
          }
          metastoreUris[i++] = tmpUri;
        }
      } catch (IllegalArgumentException e) {
        throw (e);
      } catch (Exception e) {
        String exInfo = "Got exception: " + e.getClass().getName() + " " + e.getMessage();
        log.error(exInfo, e);
        throw new RuntimeException(exInfo, e);
      }
    } else {
      log.error("NOT getting uris from conf");
      throw new RuntimeException("MetaStoreURIs not found in conf file");
    }
  }

  private static String loadDelegationToken(DelegationTokenKey key) {
    try {
      return key.client.get_delegation_token(key.username, key.username);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  private static class DelegationTokenKey{
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

  void open() {
    open(null);
  }

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
        throw new RuntimeException(e);
      }
    }
  }

  void createMetastoreClientAndOpen(String delegationToken, HiveUgiArgs ugiArgs) {
    TException te = null;
    boolean useSasl = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL);
    boolean useSsl = conf.getBoolVar(ConfVars.HIVE_METASTORE_USE_SSL);
    boolean useFramedTransport = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT);
    boolean useCompactProtocol = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL);
    int clientSocketTimeout = (int) conf.getTimeVar(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

    for (int attempt = 0; !isConnected && (attempt < retries); ++attempt) {
      for (URI store : metastoreUris) {
        log.info("Trying to connect to metastore with URI {}", store);
        try {
          transport = new TSocket(store.getHost(), store.getPort(), clientSocketTimeout, connectionTimeout);
          if (useSasl) {
            // Wrap thrift connection with SASL for secure connection.
            try {
              UserGroupInformation.setConfiguration(conf);

              // check if we should use delegation tokens to authenticate
              // the call below gets hold of the tokens if they are set up by hadoop
              // this should happen on the map/reduce tasks if the client added the
              // tokens into hadoop's credential store in the front end during job
              // submission.
//              String tokenSig = conf.getVar(ConfVars.METASTORE_TOKEN_SIGNATURE);
              // tokenSig could be null
              if (impersonationEnabled && delegationToken != null) {
                // authenticate using delegation tokens via the "DIGEST" mechanism
                transport = KerberosSaslHelper
                    .getTokenTransport(delegationToken,
                        store.getHost(), transport,
                        MetaStoreUtils.getMetaStoreSaslProperties(conf, useSsl));
              } else {
                String principalConfig = conf.getVar(ConfVars.METASTORE_KERBEROS_PRINCIPAL);
                transport = KerberosSaslHelper
                        .getKerberosTransport(principalConfig, store.getHost(), transport,
                                MetaStoreUtils.getMetaStoreSaslProperties(conf, useSsl), false);
              }
            } catch (IOException ioe) {
              log.error("Couldn't create client transport, URI " + store, ioe);
              throw new MetaException(ioe.toString());
            }
          } else if (useFramedTransport) {
            transport = new TFramedTransport(transport);
          }
          TProtocol protocol;
          if (useCompactProtocol) {
            protocol = new TCompactProtocol(transport);
          } else {
            protocol = new TBinaryProtocol(transport);
          }
          client = hiveCompatibleThriftHiveMetastoreIfaceFactory.newInstance(new ThriftHiveMetastore.Client(protocol));
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
          log.error("Unable to connect to metastore with URI " + store + " in attempt " + attempt, e);
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
        } catch (InterruptedException ignore) {}
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

  void reconnect(HiveUgiArgs ugiArgs) {
    close();
    // Swap the first element of the metastoreUris[] with a random element from the rest
    // of the array. Rationale being that this method will generally be called when the default
    // connection has died and the default connection is likely to be the first array element.
    promoteRandomMetaStoreURI();
    open(ugiArgs);
  }

  public String getHiveConfValue(String key, String defaultValue) {
    return conf.get(key, defaultValue);
  }

  public void setHiveConfValue(String key, String value) {
    conf.set(key, value);
  }

  public String generateNewTokenSignature(String defaultTokenSignature) {
    String tokenSignature = conf.get(ConfVars.METASTORE_TOKEN_SIGNATURE.varname,
            defaultTokenSignature);
    conf.set(ConfVars.METASTORE_TOKEN_SIGNATURE.varname,
            tokenSignature);
    return tokenSignature;
  }

  public Boolean isSaslEnabled() {
    return conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL);
  }

  @Override
  public void close() {
    if (!isConnected) {
      return;
    }
    isConnected = false;
    try {
      if (client != null) {
        client.shutdown();
      }
    } catch (TException e) {
      log.debug("Unable to shutdown metastore client. Will try closing transport directly.", e);
    }
    // Transport would have got closed via client.shutdown(), so we don't need this, but
    // just in case, we make this call.
    if ((transport != null) && transport.isOpen()) {
      transport.close();
      transport = null;
    }
    log.info("Closed a connection to metastore, current connections: {}", CONN_COUNT.decrementAndGet());
  }

  boolean isOpen() {
    return (transport != null) && transport.isOpen();
  }

  protected ThriftHiveMetastore.Iface getClient() {
    return client;
  }

  /**
   * Swaps the first element of the metastoreUris array with a random element from the remainder of the array.
   */
  private void promoteRandomMetaStoreURI() {
    if (metastoreUris.length <= 1) {
      return;
    }
    Random rng = new Random();
    int index = rng.nextInt(metastoreUris.length - 1) + 1;
    URI tmp = metastoreUris[0];
    metastoreUris[0] = metastoreUris[index];
    metastoreUris[index] = tmp;
  }
}
