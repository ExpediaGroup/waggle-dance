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

import java.util.List;

import org.apache.hadoop.hive.metastore.api.AddCheckConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDefaultConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AddForeignKeyRequest;
import org.apache.hadoop.hive.metastore.api.AddNotNullConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AddUniqueConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AlterISchemaRequest;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.DropConstraintRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
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
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.MapSchemaVersionToSerdeRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.SetSchemaVersionStateRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;

public interface DatabaseMapping extends MetaStoreMapping {

  ISchema transformInboundISchema(ISchema iSchema);

  Partition transformInboundPartition(Partition partition);

  Table transformInboundTable(Table table);

  HiveObjectRef transformInboundHiveObjectRef(HiveObjectRef function);

  ISchema transformOutboundISchema(ISchema iSchema);

  Partition transformOutboundPartition(Partition partition);

  Table transformOutboundTable(Table table);

  Function transformOutboundFunction(Function function);

  HiveObjectRef transformOutboundHiveObjectRef(HiveObjectRef function);

  PartitionsStatsRequest transformInboundPartitionsStatsRequest(PartitionsStatsRequest request);

  TableStatsRequest transformInboundTableStatsRequest(TableStatsRequest request);

  PartitionSpec transformOutboundPartitionSpec(PartitionSpec partitionSpec);

  PartitionsByExprRequest transformInboundPartitionsByExprRequest(PartitionsByExprRequest req);

  PartitionsByExprResult transformOutboundPartitionsByExprResult(PartitionsByExprResult result);

  CacheFileMetadataRequest transformInboundCacheFileMetadataRequest(CacheFileMetadataRequest req);

  FireEventRequest transformInboundFireEventRequest(FireEventRequest request);

  ForeignKeysRequest transformInboundForeignKeysRequest(ForeignKeysRequest request);

  ForeignKeysResponse transformOutboundForeignKeysResponse(ForeignKeysResponse response);

  PrimaryKeysRequest transformInboundPrimaryKeysRequest(PrimaryKeysRequest request);

  PrimaryKeysResponse transformOutboundPrimaryKeysResponse(PrimaryKeysResponse response);

  TableMeta transformOutboundTableMeta(TableMeta tableMeta);

  AddDynamicPartitions transformInboundAddDynamicPartitions(AddDynamicPartitions request);

  DropConstraintRequest transformInboundDropConstraintRequest(DropConstraintRequest request);

  AddPartitionsRequest transformInboundAddPartitionsRequest(AddPartitionsRequest request);

  AddPartitionsResult transformOutboundAddPartitionsResult(AddPartitionsResult addPartitionsResult);

  DropPartitionsRequest transformInboundDropPartitionRequest(DropPartitionsRequest req);

  DropPartitionsResult transformOutboundDropPartitionsResult(DropPartitionsResult dropPartitionsResult);

  List<Partition> transformOutboundPartitions(List<Partition> partitions);

  List<PartitionSpec> transformOutboundPartitionSpecs(List<PartitionSpec> partitionSpecs);

  List<Partition> transformInboundPartitions(List<Partition> partitions);

  List<ISchema> transformOutboundISchemas(List<ISchema> iSchemas);

  ColumnStatistics transformInboundColumnStatistics(ColumnStatistics columnStatistics);

  ColumnStatistics transformOutboundColumnStatistics(ColumnStatistics columnStatistics);

  SetPartitionsStatsRequest transformInboundSetPartitionStatsRequest(SetPartitionsStatsRequest request);

  Function transformInboundFunction(Function function);

  List<HiveObjectPrivilege> transformOutboundHiveObjectPrivileges(List<HiveObjectPrivilege> privileges);

  PrivilegeBag transformInboundPrivilegeBag(PrivilegeBag privilegeBag);

  GrantRevokePrivilegeRequest transformInboundGrantRevokePrivilegesRequest(GrantRevokePrivilegeRequest request);

  LockRequest transformInboundLockRequest(LockRequest request);

  CompactionRequest transformInboundCompactionRequest(CompactionRequest request);

  Database transformInboundDatabase(Database database);

  List<PartitionSpec> transformInboundPartitionSpecs(List<PartitionSpec> partitionSpecs);

  GetTableRequest transformInboundGetTableRequest(GetTableRequest request);

  GetTableResult transformOutboundGetTableResult(GetTableResult result);

  GetTablesRequest transformInboundGetTablesRequest(GetTablesRequest req);

  GetTablesResult transformOutboundGetTablesResult(GetTablesResult result);

  PartitionValuesRequest transformInboundPartitionValuesRequest(PartitionValuesRequest req);

  List<SQLPrimaryKey> transformInboundSQLPrimaryKeys(List<SQLPrimaryKey> sqlPrimaryKeys);

  List<SQLForeignKey> transformInboundSQLForeignKeys(List<SQLForeignKey> sqlForeignKeys);

  List<SQLUniqueConstraint> transformInboundSQLUniqueConstraints(List<SQLUniqueConstraint> sqlUniqueConstraints);

  List<SQLNotNullConstraint> transformInboundSQLNotNullConstraints(List<SQLNotNullConstraint> sqlNotNullConstraints);

  List<SQLDefaultConstraint> transformInboundSQLDefaultConstraints(List<SQLDefaultConstraint> sqlDefaultConstraints);

  List<SQLCheckConstraint> transformInboundSQLCheckConstraints(List<SQLCheckConstraint> sqlCheckConstraints);

  ReplTblWriteIdStateRequest transformInboundReplTblWriteIdStateRequest(ReplTblWriteIdStateRequest request);

  AllocateTableWriteIdsRequest transformInboundAllocateTableWriteIdsRequest(AllocateTableWriteIdsRequest request);

  AlterISchemaRequest transformInboundAlterISchemaRequest(AlterISchemaRequest request);

  SchemaVersion transformInboundSchemaVersion(SchemaVersion schemaVersion);

  SchemaVersion transformOutboundSchemaVersion(SchemaVersion schemaVersion);

  List<SchemaVersion> transformOutboundSchemaVersions(List<SchemaVersion> schemaVersions);

  ISchemaName transformInboundISchemaName(ISchemaName iSchemaName);

  ISchemaName transformOutboundISchemaName(ISchemaName iSchemaName);

  AddForeignKeyRequest transformInboundAddForeignKeyRequest(AddForeignKeyRequest request);

  AddUniqueConstraintRequest transformInboundAddUniqueConstraintRequest(AddUniqueConstraintRequest request);

  AddNotNullConstraintRequest transformInboundAddNotNullConstraintRequest(AddNotNullConstraintRequest request);

  AddDefaultConstraintRequest transformInboundAddDefaultConstraintRequest(AddDefaultConstraintRequest request);

  AddCheckConstraintRequest transformInboundAddCheckConstraintRequest(AddCheckConstraintRequest request);

  FindSchemasByColsResp transformOutboundFindSchemasByColsResp(FindSchemasByColsResp response);

  SchemaVersionDescriptor transformInboundSchemaVersionDescriptor(SchemaVersionDescriptor request);

  MapSchemaVersionToSerdeRequest transformInboundMapSchemaVersionToSerdeRequest(MapSchemaVersionToSerdeRequest request);

  SetSchemaVersionStateRequest transformInboundSetSchemaVersionStateRequest(SetSchemaVersionStateRequest request);

  NotificationEventsCountRequest transformInboundNotificationEventsCountRequest(NotificationEventsCountRequest request);

  UniqueConstraintsRequest transformInboundUniqueConstraintsRequest(UniqueConstraintsRequest request);

  UniqueConstraintsResponse transformOutboundUniqueConstraintsResponse(UniqueConstraintsResponse response);

  NotNullConstraintsRequest transformInboundNotNullConstraintsRequest(NotNullConstraintsRequest request);

  NotNullConstraintsResponse transformOutboundNotNullConstraintsResponse(NotNullConstraintsResponse response);

  DefaultConstraintsRequest transformInboundDefaultConstraintsRequest(DefaultConstraintsRequest request);

  DefaultConstraintsResponse transformOutboundDefaultConstraintsResponse(DefaultConstraintsResponse response);

  CheckConstraintsRequest transformInboundCheckConstraintsRequest(CheckConstraintsRequest request);

  CheckConstraintsResponse transformOutboundCheckConstraintsResponse(CheckConstraintsResponse response);

  CreationMetadata transformInboundCreationMetadata(CreationMetadata request);

  @Override
  long getLatency();
}
