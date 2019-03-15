/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import java.io.Closeable;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;

import com.hotels.bdp.waggledance.server.security.NotAllowedException;

public interface MetaStoreMapping extends Closeable {

  String transformOutboundDatabaseName(String databaseName);

  Database transformOutboundDatabase(Database database);

  String transformInboundDatabaseName(String databaseName);

  ThriftHiveMetastore.Iface getClient();

  String getDatabasePrefix();

  String getMetastoreMappingName();

  boolean isAvailable();

  /**
   * @param databaseName (assumed to be the database name used in this mapped metastore so any waggle dance related
   *          prefix must have been stripped already)
   * @return this, throws {@link NotAllowedException} when the Metastore mapped by this class does not have write
   *         permissions for the database
   */
  MetaStoreMapping checkWritePermissions(String databaseName);

  /**
   * @param database (assumed to be the database used in this mapped metastore so any waggle dance related prefix must
   *          have been stripped already)
   * @throws NotAllowedException when the metastore mapped by this class does not have the correct permission otherwise
   *           calls getClient().create_database(database)
   * @throws TException if a Thrift error occurs
   * @throws MetaException if a problem occurs interacting with the Hive MetaStore
   * @throws InvalidObjectException if Hive considers the database invalid
   * @throws AlreadyExistsException if the database already exists
   */
  void createDatabase(Database database)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException;

  long getLatency();
}
