/**
 * Copyright (C) 2016-2017 Expedia Inc.
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

import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropConstraintRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;

public interface DatabaseMapping extends MetaStoreMapping {

  Index transformInboundIndex(Index index);

  Partition transformInboundPartition(Partition partition);

  Table transformInboundTable(Table table);

  HiveObjectRef transformInboundHiveObjectRef(HiveObjectRef function);

  Index transformOutboundIndex(Index index);

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

  PrimaryKeysRequest transformInboundPrimaryKeysRequest(PrimaryKeysRequest request);

  TableMeta transformOutboundTableMeta(TableMeta tableMeta);

  AddDynamicPartitions transformInboundAddDynamicPartitions(AddDynamicPartitions request);

  DropConstraintRequest transformInboundDropConstraintRequest(DropConstraintRequest request);

  AddPartitionsRequest transformInboundAddPartitionsRequest(AddPartitionsRequest request);

  AddPartitionsResult transformOutboundAddPartitionsResult(AddPartitionsResult addPartitionsResult);

  DropPartitionsRequest transformInboundDropPartitionRequest(DropPartitionsRequest req);

  DropPartitionsResult transforOutboundDropPartitionsResult(DropPartitionsResult dropPartitionsResult);

  List<Partition> transformOutboundPartitions(List<Partition> partitions);

  List<PartitionSpec> transformOutboundPartitionSpecs(List<PartitionSpec> partitionSpecs);

  List<Partition> transformInboundPartitions(List<Partition> partitions);

  List<Index> transformOutboundIndexes(List<Index> indexes);

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
}
