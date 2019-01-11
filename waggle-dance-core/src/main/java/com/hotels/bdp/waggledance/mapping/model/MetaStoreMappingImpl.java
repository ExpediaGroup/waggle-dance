/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
import java.util.Locale;

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

  private final String databasePrefix;
  private final CloseableThriftHiveMetastoreIface client;
  private final AccessControlHandler accessControlHandler;
  private final String name;

  private final ConnectionType connectionType;

  MetaStoreMappingImpl(
      String databasePrefix,
      String name,
      CloseableThriftHiveMetastoreIface client,
      AccessControlHandler accessControlHandler,
      ConnectionType connectionType) {
    this.databasePrefix = databasePrefix;
    this.name = name;
    this.client = client;
    this.accessControlHandler = accessControlHandler;
    this.connectionType = connectionType;
  }

  @Override
  public String transformOutboundDatabaseName(String databaseName) {
    return getDatabasePrefix() + databaseName.toLowerCase(Locale.ROOT);
  }

  @Override
  public ThriftHiveMetastore.Iface getClient() {
    return client;
  }

  @Override
  public Database transformOutboundDatabase(Database database) {
    database.setName(transformOutboundDatabaseName(database.getName()));
    return database;
  }

  @Override
  public String transformInboundDatabaseName(String databaseName) {
    databaseName = databaseName.toLowerCase(Locale.ROOT);
    if (!databaseName.startsWith(getDatabasePrefix())) {
      throw new IllegalArgumentException(
          "Database '" + databaseName + "' does not start with prefix '" + getDatabasePrefix() + "'");
    }
    return databaseName.substring(getDatabasePrefix().length());
  }

  @Override
  public String getDatabasePrefix() {
    return databasePrefix;
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  @Override
  public boolean isAvailable() {
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
}
