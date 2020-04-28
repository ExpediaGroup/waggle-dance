/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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

import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropConstraintRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;

public class IdentityMapping implements DatabaseMapping {

  static final String EMPTY_PREFIX = "";

  private final MetaStoreMapping metaStoreMapping;

  public IdentityMapping(MetaStoreMapping metaStoreMapping) {
    this.metaStoreMapping = metaStoreMapping;
  }

  @Override
  public Table transformOutboundTable(Table table) {
    return table;
  }

  @Override
  public Partition transformOutboundPartition(Partition partition) {
    return partition;
  }

  @Override
  public Index transformOutboundIndex(Index index) {
    return index;
  }

  @Override
  public String transformOutboundDatabaseName(String databaseName) {
    return databaseName;
  }

  @Override
  public Table transformInboundTable(Table table) {
    return table;
  }

  @Override
  public Partition transformInboundPartition(Partition partition) {
    return partition;
  }

  @Override
  public Index transformInboundIndex(Index index) {
    return index;
  }

  @Override
  public String transformInboundDatabaseName(String databaseName) {
    return databaseName;
  }

  @Override
  public ThriftHiveMetastore.Iface getClient() {
    return metaStoreMapping.getClient();
  }

  @Override
  public Database transformOutboundDatabase(Database database) {
    return database;
  }

  @Override
  public Function transformOutboundFunction(Function function) {
    return function;
  }

  @Override
  public HiveObjectRef transformInboundHiveObjectRef(HiveObjectRef obj) {
    return obj;
  }

  @Override
  public HiveObjectRef transformOutboundHiveObjectRef(HiveObjectRef obj) {
    return obj;
  }

  @Override
  public PartitionsStatsRequest transformInboundPartitionsStatsRequest(PartitionsStatsRequest request) {
    return request;
  }

  @Override
  public TableStatsRequest transformInboundTableStatsRequest(TableStatsRequest request) {
    return request;
  }

  @Override
  public PartitionSpec transformOutboundPartitionSpec(PartitionSpec request) {
    return request;
  }

  @Override
  public PartitionsByExprRequest transformInboundPartitionsByExprRequest(PartitionsByExprRequest req) {
    return req;
  }

  @Override
  public PartitionsByExprResult transformOutboundPartitionsByExprResult(PartitionsByExprResult result) {
    return result;
  }

  @Override
  public String getDatabasePrefix() {
    return EMPTY_PREFIX;
  }

  @Override
  public String getMetastoreMappingName() {
    return metaStoreMapping.getMetastoreMappingName();
  }

  @Override
  public CacheFileMetadataRequest transformInboundCacheFileMetadataRequest(CacheFileMetadataRequest req) {
    return req;
  }

  @Override
  public FireEventRequest transformInboundFireEventRequest(FireEventRequest rqst) {
    return rqst;
  }

  @Override
  public ForeignKeysRequest transformInboundForeignKeysRequest(ForeignKeysRequest request) {
    return request;
  }

  @Override
  public ForeignKeysResponse transformOutboundForeignKeysResponse(ForeignKeysResponse response) {
    return response;
  }

  @Override
  public PrimaryKeysRequest transformInboundPrimaryKeysRequest(PrimaryKeysRequest request) {
    return request;
  }

  @Override
  public PrimaryKeysResponse transformOutboundPrimaryKeysResponse(PrimaryKeysResponse response) {
    return response;
  }

  @Override
  public TableMeta transformOutboundTableMeta(TableMeta tableMeta) {
    return tableMeta;
  }

  @Override
  public void close() throws IOException {
    metaStoreMapping.close();
  }

  @Override
  public boolean isAvailable() {
    return metaStoreMapping.isAvailable();
  }

  @Override
  public MetaStoreMapping checkWritePermissions(String databaseName) throws IllegalArgumentException {
    return metaStoreMapping.checkWritePermissions(databaseName);
  }

  @Override
  public AddDynamicPartitions transformInboundAddDynamicPartitions(AddDynamicPartitions request) {
    return request;
  }

  @Override
  public DropConstraintRequest transformInboundDropConstraintRequest(DropConstraintRequest request) {
    return request;
  }

  @Override
  public AddPartitionsRequest transformInboundAddPartitionsRequest(AddPartitionsRequest request) {
    return request;
  }

  @Override
  public AddPartitionsResult transformOutboundAddPartitionsResult(AddPartitionsResult addPartitionsResult) {
    return addPartitionsResult;
  }

  @Override
  public DropPartitionsRequest transformInboundDropPartitionRequest(DropPartitionsRequest request) {
    return request;
  }

  @Override
  public DropPartitionsResult transformOutboundDropPartitionsResult(DropPartitionsResult dropPartitionsResult) {
    return dropPartitionsResult;
  }

  @Override
  public List<Partition> transformOutboundPartitions(List<Partition> partitions) {
    return partitions;
  }

  @Override
  public List<PartitionSpec> transformOutboundPartitionSpecs(List<PartitionSpec> partitionSpecs) {
    return partitionSpecs;
  }

  @Override
  public List<Partition> transformInboundPartitions(List<Partition> partitions) {
    return partitions;
  }

  @Override
  public List<Index> transformOutboundIndexes(List<Index> indexes) {
    return indexes;
  }

  @Override
  public ColumnStatistics transformInboundColumnStatistics(ColumnStatistics columnStatistics) {
    return columnStatistics;
  }

  @Override
  public ColumnStatistics transformOutboundColumnStatistics(ColumnStatistics columnStatistics) {
    return columnStatistics;
  }

  @Override
  public SetPartitionsStatsRequest transformInboundSetPartitionStatsRequest(SetPartitionsStatsRequest request) {
    return request;
  }

  @Override
  public Function transformInboundFunction(Function function) {
    return function;
  }

  @Override
  public List<HiveObjectPrivilege> transformOutboundHiveObjectPrivileges(List<HiveObjectPrivilege> privileges) {
    return privileges;
  }

  @Override
  public PrivilegeBag transformInboundPrivilegeBag(PrivilegeBag privilegeBag) {
    return privilegeBag;
  }

  @Override
  public GrantRevokePrivilegeRequest transformInboundGrantRevokePrivilegesRequest(GrantRevokePrivilegeRequest request) {
    return request;
  }

  @Override
  public LockRequest transformInboundLockRequest(LockRequest request) {
    return request;
  }

  @Override
  public CompactionRequest transformInboundCompactionRequest(CompactionRequest request) {
    return request;
  }

  @Override
  public void createDatabase(Database database)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    metaStoreMapping.createDatabase(database);
  }

  @Override
  public Database transformInboundDatabase(Database database) {
    return database;
  }

  @Override
  public List<PartitionSpec> transformInboundPartitionSpecs(List<PartitionSpec> partitionSpecs) {
    return partitionSpecs;
  }

  @Override
  public GetTableRequest transformInboundGetTableRequest(GetTableRequest request) {
    return request;
  }

  @Override
  public GetTableResult transformOutboundGetTableResult(GetTableResult result) {
    return result;
  }

  @Override
  public GetTablesRequest transformInboundGetTablesRequest(GetTablesRequest request) {
    return request;
  }

  @Override
  public GetTablesResult transformOutboundGetTablesResult(GetTablesResult result) {
    return result;
  }

  @Override
  public PartitionValuesRequest transformInboundPartitionValuesRequest(PartitionValuesRequest request) {
    return request;
  }

  @Override
  public long getLatency() {
    return metaStoreMapping.getLatency();
  }

}
