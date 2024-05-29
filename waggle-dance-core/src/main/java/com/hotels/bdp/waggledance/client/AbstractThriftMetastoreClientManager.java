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

import java.io.Closeable;
import java.net.URI;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfUtil;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransport;

import lombok.extern.log4j.Log4j2;

import com.hotels.bdp.waggledance.client.compatibility.HiveCompatibleThriftHiveMetastoreIfaceFactory;

@Log4j2
public abstract class AbstractThriftMetastoreClientManager implements Closeable {

  protected static final AtomicInteger CONN_COUNT = new AtomicInteger(0);
  protected final HiveConf conf;
  protected final HiveCompatibleThriftHiveMetastoreIfaceFactory hiveCompatibleThriftHiveMetastoreIfaceFactory;
  protected final URI[] metastoreUris;
  protected ThriftHiveMetastore.Iface client = null;
  protected TTransport transport = null;
  protected boolean isConnected = false;
  // for thrift connects
  protected int retries = 5;
  protected long retryDelaySeconds = 0;

  protected final int connectionTimeout;
  protected final String msUri;

  AbstractThriftMetastoreClientManager(
      HiveConf conf,
      HiveCompatibleThriftHiveMetastoreIfaceFactory hiveCompatibleThriftHiveMetastoreIfaceFactory,
      int connectionTimeout) {
    this.conf = conf;
    this.hiveCompatibleThriftHiveMetastoreIfaceFactory = hiveCompatibleThriftHiveMetastoreIfaceFactory;
    this.connectionTimeout = connectionTimeout;
    msUri = conf.getVar(ConfVars.METASTOREURIS);

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

  void open() {
    open(null);
  }

  abstract void open(HiveUgiArgs ugiArgs);

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
