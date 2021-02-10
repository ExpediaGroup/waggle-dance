/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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
import java.util.List;

import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.thrift.TException;

public abstract class MetaStoreMappingDecorator implements MetaStoreMapping {

  private final MetaStoreMapping metaStoreMapping;

  public MetaStoreMappingDecorator(MetaStoreMapping metaStoreMapping) {
    this.metaStoreMapping = metaStoreMapping;
  }

  @Override
  public String transformOutboundDatabaseName(String databaseName) {
    return metaStoreMapping.transformOutboundDatabaseName(databaseName);
  }

  @Override
  public List<String> transformOutboundDatabaseNameMultiple(String databaseName) {
    return metaStoreMapping.transformOutboundDatabaseNameMultiple(databaseName);
  }

  @Override
  public Database transformOutboundDatabase(Database database) {
    return metaStoreMapping.transformOutboundDatabase(database);
  }

  @Override
  public String transformInboundDatabaseName(String databaseName) {
    return metaStoreMapping.transformInboundDatabaseName(databaseName);
  }

  @Override
  public void close() throws IOException {
    metaStoreMapping.close();
  }

  @Override
  public Iface getClient() {
    return metaStoreMapping.getClient();
  }

  @Override
  public MetaStoreFilterHook getMetastoreFilter() {
    return metaStoreMapping.getMetastoreFilter();
  }

  @Override
  public String getDatabasePrefix() {
    return metaStoreMapping.getDatabasePrefix();
  }

  @Override
  public String getMetastoreMappingName() {
    return metaStoreMapping.getMetastoreMappingName();
  }

  @Override
  public boolean isAvailable() {
    return metaStoreMapping.isAvailable();
  }

  @Override
  public MetaStoreMapping checkWritePermissions(String databaseName) {
    return metaStoreMapping.checkWritePermissions(databaseName);
  }

  @Override
  public void createDatabase(Database database)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    metaStoreMapping.createDatabase(database);
  }

  @Override
  public long getLatency() {
    return metaStoreMapping.getLatency();
  }

}
