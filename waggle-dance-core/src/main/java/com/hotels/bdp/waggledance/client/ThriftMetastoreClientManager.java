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
package com.hotels.bdp.waggledance.client;


import java.net.URI;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.waggledance.client.compatibility.HiveCompatibleThriftHiveMetastoreIfaceFactory;


class ThriftMetastoreClientManager extends AbstractThriftMetastoreClientManager {

  private static final Logger log = LoggerFactory.getLogger(ThriftMetastoreClientManager.class);

  ThriftMetastoreClientManager(
      HiveConf conf,
      HiveCompatibleThriftHiveMetastoreIfaceFactory hiveCompatibleThriftHiveMetastoreIfaceFactory,
      int connectionTimeout) {
    super(conf, hiveCompatibleThriftHiveMetastoreIfaceFactory, connectionTimeout);
  }

  void open(HiveUgiArgs ugiArgs) throws TException {
    if (isConnected) {
      return;
    }
    TException te = null;
    boolean useFramedTransport = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT);
    boolean useCompactProtocol = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL);
    int clientSocketTimeout = (int) conf.getTimeVar(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);

    for (int attempt = 0; !isConnected && (attempt < retries); ++attempt) {
      for (URI store : metastoreUris) {
        log.debug("Trying to connect to metastore with URI {}", store);
        transport = new TSocket(store.getHost(), store.getPort(), clientSocketTimeout, connectionTimeout);
        if (useFramedTransport) {
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
      log.debug("Could not connect to meta store using any of the URIs ["
          + msUri
          + "] provided. Most recent failure: "
          + StringUtils.stringifyException(te), te);
      throw te;
    }
    log.debug("Connected to metastore.");
  }
}
