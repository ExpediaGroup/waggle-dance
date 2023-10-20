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
package com.hotels.bdp.waggledance.mapping.model;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.waggledance.api.model.ConnectionType;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.server.security.AccessControlHandler;
import com.hotels.bdp.waggledance.server.security.NotAllowedException;

class MetaStoreMappingImpl implements MetaStoreMapping {

  private final static Logger log = LoggerFactory.getLogger(MetaStoreMappingImpl.class);

  // MilliSeconds
  static final long DEFAULT_AVAILABILITY_TIMEOUT = 500;

  private final String databasePrefix;
  private final CloseableThriftHiveMetastoreIface client;
  private final AccessControlHandler accessControlHandler;
  private final String name;
  private final long latency;
  private final MetaStoreFilterHook metastoreFilter;

  private final ConnectionType connectionType;

  MetaStoreMappingImpl(
      String databasePrefix,
      String name,
      CloseableThriftHiveMetastoreIface client,
      AccessControlHandler accessControlHandler,
      ConnectionType connectionType,
      long latency,
      MetaStoreFilterHook metastoreFilter) {
    this.databasePrefix = databasePrefix;
    this.name = name;
    this.client = client;
    this.accessControlHandler = accessControlHandler;
    this.connectionType = connectionType;
    this.latency = latency;
    this.metastoreFilter = metastoreFilter;
  }

  @Override
  public String transformOutboundDatabaseName(String databaseName) {
    return databaseName.toLowerCase(Locale.ROOT);
  }

  @Override
  public List<String> transformOutboundDatabaseNameMultiple(String databaseName) {
    return Collections.singletonList(transformInboundDatabaseName(databaseName));
  }

  @Override
  public ThriftHiveMetastore.Iface getClient() {
    return client;
  }

  @Override
  public MetaStoreFilterHook getMetastoreFilter() {
    return metastoreFilter;
  }

  @Override
  public Database transformOutboundDatabase(Database database) {
    database.setName(transformOutboundDatabaseName(database.getName()));
    return database;
  }

  @Override
  public String transformInboundDatabaseName(String databaseName) {
    return databaseName.toLowerCase(Locale.ROOT);
  }

  @Override
  public String getDatabasePrefix() {
    return databasePrefix;
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  /**
   * This is potentially slow so a best effort is made and false is returned after a timeout.
   */
  @Override
  public boolean isAvailable() {
    Future<Boolean> future = CompletableFuture.supplyAsync(() -> {
      try {
        boolean isOpen = client.isOpen();
        if (isOpen && connectionType == ConnectionType.TUNNELED) {
          client.getStatus();
        }
        return isOpen;
      } catch (Exception e) {
        log.error("Metastore Mapping {} unavailable", name, e);
        return false;
      }
    });
    try {
      return future.get(DEFAULT_AVAILABILITY_TIMEOUT + getLatency(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      log.info("Took too long to check availability of '" + name + "', assuming unavailable");
      future.cancel(true);
    } catch (InterruptedException | ExecutionException e) {
      log.error("Error while checking availability '" + name + "', assuming unavailable");
    }
    return false;
  }

  @Override
  public MetaStoreMapping checkWritePermissions(String databaseName) {
    if (!accessControlHandler.hasWritePermission(databaseName)) {
      throw new NotAllowedException(
          "You cannot perform this operation on the virtual database '" + databaseName + "'.");
    }
    return this;
  }

  @Override
  public void createDatabase(Database database)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    if (accessControlHandler.hasCreatePermission()) {
      getClient().create_database(database);
      accessControlHandler.databaseCreatedNotification(database.getName());
    } else {
      throw new NotAllowedException("You cannot create the database '" + database.getName() + "'.");
    }
  }

  @Override
  public String getMetastoreMappingName() {
    return name;
  }

  @Override
  public long getLatency() {
    return latency;
  }

}
