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

import java.io.IOException;
import java.util.ArrayList;
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
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
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
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.thrift.TException;

public class DatabaseMappingImpl implements DatabaseMapping {

  private final MetaStoreMapping metaStoreMapping;

  public DatabaseMappingImpl(MetaStoreMapping metaStoreMapping) {
    this.metaStoreMapping = metaStoreMapping;
  }

  @Override
  public Table transformOutboundTable(Table table) {
    Table outbound = new Table(table);
    outbound.setDbName(metaStoreMapping.transformOutboundDatabaseName(table.getDbName()));
    return outbound;
  }

  @Override
  public Partition transformOutboundPartition(Partition partition) {
    Partition outbound = new Partition(partition);
    outbound.setDbName(metaStoreMapping.transformOutboundDatabaseName(partition.getDbName()));
    return outbound;
  }

  @Override
  public Index transformOutboundIndex(Index index) {
    Index outbound = new Index(index);
    outbound.setDbName(metaStoreMapping.transformOutboundDatabaseName(index.getDbName()));
    return outbound;
  }

  @Override
  public Table transformInboundTable(Table table) {
    Table inbound = new Table(table);
    inbound.setDbName(metaStoreMapping.transformInboundDatabaseName(table.getDbName()));
    return inbound;
  }

  @Override
  public Partition transformInboundPartition(Partition partition) {
    Partition outbound = new Partition(partition);
    outbound.setDbName(metaStoreMapping.transformInboundDatabaseName(partition.getDbName()));
    return outbound;
  }

  @Override
  public Index transformInboundIndex(Index index) {
    Index outbound = new Index(index);
    outbound.setDbName(metaStoreMapping.transformInboundDatabaseName(index.getDbName()));
    return outbound;
  }

  @Override
  public Function transformOutboundFunction(Function function) {
    Function outbound = new Function(function);
    outbound.setDbName(metaStoreMapping.transformOutboundDatabaseName(function.getDbName()));
    return outbound;
  }

  @Override
  public HiveObjectRef transformInboundHiveObjectRef(HiveObjectRef obj) {
    HiveObjectRef inbound = new HiveObjectRef(obj);
    inbound.setDbName(metaStoreMapping.transformInboundDatabaseName(obj.getDbName()));
    if (inbound.getObjectType() == HiveObjectType.DATABASE) {
      inbound.setObjectName(metaStoreMapping.transformInboundDatabaseName(obj.getDbName()));
    }
    return inbound;
  }

  @Override
  public HiveObjectRef transformOutboundHiveObjectRef(HiveObjectRef obj) {
    HiveObjectRef outbound = new HiveObjectRef(obj);
    outbound.setDbName(metaStoreMapping.transformOutboundDatabaseName(obj.getDbName()));
    if (outbound.getObjectType() == HiveObjectType.DATABASE) {
      outbound.setObjectName(metaStoreMapping.transformOutboundDatabaseName(obj.getObjectName()));
    }
    return outbound;
  }

  @Override
  public PartitionSpec transformOutboundPartitionSpec(PartitionSpec partitionSpec) {
    PartitionSpec outbound = new PartitionSpec(partitionSpec);
    outbound.setDbName(metaStoreMapping.transformOutboundDatabaseName(partitionSpec.getDbName()));
    return outbound;
  }

  @Override
  public PartitionsStatsRequest transformInboundPartitionsStatsRequest(PartitionsStatsRequest request) {
    PartitionsStatsRequest inbound = new PartitionsStatsRequest(request);
    inbound.setDbName(metaStoreMapping.transformInboundDatabaseName(request.getDbName()));
    return inbound;
  }

  @Override
  public TableStatsRequest transformInboundTableStatsRequest(TableStatsRequest request) {
    TableStatsRequest inbound = new TableStatsRequest(request);
    inbound.setDbName(metaStoreMapping.transformInboundDatabaseName(request.getDbName()));
    return inbound;
  }

  @Override
  public PartitionsByExprRequest transformInboundPartitionsByExprRequest(PartitionsByExprRequest req) {
    PartitionsByExprRequest inbound = new PartitionsByExprRequest(req);
    inbound.setDbName(metaStoreMapping.transformInboundDatabaseName(req.getDbName()));
    return inbound;
  }

  @Override
  public PartitionsByExprResult transformOutboundPartitionsByExprResult(PartitionsByExprResult result) {
    PartitionsByExprResult outbound = new PartitionsByExprResult(result);
    outbound.setPartitions(transformOutboundPartitions(result.getPartitions()));
    return outbound;
  }

  @Override
  public Iface getClient() {
    return metaStoreMapping.getClient();
  }

  @Override
  public String transformOutboundDatabaseName(String databaseName) {
    return metaStoreMapping.transformOutboundDatabaseName(databaseName);
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
  public String getDatabasePrefix() {
    return metaStoreMapping.getDatabasePrefix();
  }

  @Override
  public String getMetastoreMappingName() {
    return metaStoreMapping.getMetastoreMappingName();
  }

  @Override
  public CacheFileMetadataRequest transformInboundCacheFileMetadataRequest(CacheFileMetadataRequest req) {
    CacheFileMetadataRequest inbound = new CacheFileMetadataRequest(req);
    inbound.setDbName(metaStoreMapping.transformInboundDatabaseName(req.getDbName()));
    return inbound;
  }

  @Override
  public FireEventRequest transformInboundFireEventRequest(FireEventRequest rqst) {
    FireEventRequest inbound = new FireEventRequest(rqst);
    inbound.setDbName(metaStoreMapping.transformInboundDatabaseName(rqst.getDbName()));
    return inbound;
  }

  @Override
  public ForeignKeysRequest transformInboundForeignKeysRequest(ForeignKeysRequest request) {
    ForeignKeysRequest inbound = new ForeignKeysRequest(request);
    inbound.setParent_db_name(metaStoreMapping.transformInboundDatabaseName(request.getParent_db_name()));
    inbound.setForeign_db_name(metaStoreMapping.transformInboundDatabaseName(request.getForeign_db_name()));
    return inbound;
  }

  @Override
  public PrimaryKeysRequest transformInboundPrimaryKeysRequest(PrimaryKeysRequest request) {
    PrimaryKeysRequest inbound = new PrimaryKeysRequest(request);
    inbound.setDb_name(metaStoreMapping.transformInboundDatabaseName(request.getDb_name()));
    return inbound;
  }

  @Override
  public TableMeta transformOutboundTableMeta(TableMeta tableMeta) {
    TableMeta outbound = new TableMeta(tableMeta);
    outbound.setDbName(metaStoreMapping.transformOutboundDatabaseName(tableMeta.getDbName()));
    return outbound;
  }

  @Override
  public AddDynamicPartitions transformInboundAddDynamicPartitions(AddDynamicPartitions request) {
    AddDynamicPartitions inbound = new AddDynamicPartitions(request);
    inbound.setDbname(metaStoreMapping.transformInboundDatabaseName(request.getDbname()));
    return inbound;
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
  public DropConstraintRequest transformInboundDropConstraintRequest(DropConstraintRequest request) {
    DropConstraintRequest inbound = new DropConstraintRequest(request);
    inbound.setDbname(metaStoreMapping.transformInboundDatabaseName(request.getDbname()));
    return inbound;
  }

  @Override
  public AddPartitionsRequest transformInboundAddPartitionsRequest(AddPartitionsRequest request) {
    AddPartitionsRequest inbound = new AddPartitionsRequest(request);
    inbound.setDbName(metaStoreMapping.transformInboundDatabaseName(request.getDbName()));
    inbound.setParts(transformInboundPartitions(request.getParts()));
    return inbound;
  }

  @Override
  public AddPartitionsResult transformOutboundAddPartitionsResult(AddPartitionsResult result) {
    AddPartitionsResult outbound = new AddPartitionsResult(result);
    outbound.setPartitions(transformOutboundPartitions(result.getPartitions()));
    return outbound;
  }

  @Override
  public DropPartitionsRequest transformInboundDropPartitionRequest(DropPartitionsRequest request) {
    DropPartitionsRequest inbound = new DropPartitionsRequest(request);
    inbound.setDbName(metaStoreMapping.transformInboundDatabaseName(request.getDbName()));
    return inbound;
  }

  @Override
  public DropPartitionsResult transforOutboundDropPartitionsResult(DropPartitionsResult result) {
    DropPartitionsResult outbound = new DropPartitionsResult(result);
    outbound.setPartitions(transformOutboundPartitions(result.getPartitions()));
    return outbound;
  }

  @Override
  public List<Partition> transformOutboundPartitions(List<Partition> partitions) {
    List<Partition> outboundPartitions = new ArrayList<>(partitions.size());
    for (Partition partition : partitions) {
      outboundPartitions.add(transformOutboundPartition(partition));
    }
    return outboundPartitions;
  }

  @Override
  public List<PartitionSpec> transformOutboundPartitionSpecs(List<PartitionSpec> partitionSpecs) {
    List<PartitionSpec> outboundPartitionSpecs = new ArrayList<>(partitionSpecs.size());
    for (PartitionSpec partitionSpec : partitionSpecs) {
      outboundPartitionSpecs.add(transformOutboundPartitionSpec(partitionSpec));
    }
    return outboundPartitionSpecs;
  }

  @Override
  public List<Partition> transformInboundPartitions(List<Partition> partitions) {
    List<Partition> inboundPartitions = new ArrayList<>(partitions.size());
    for (Partition partition : partitions) {
      inboundPartitions.add(transformInboundPartition(partition));
    }
    return inboundPartitions;
  }

  @Override
  public List<Index> transformOutboundIndexes(List<Index> indexes) {
    List<Index> outbound = new ArrayList<>(indexes.size());
    for (Index index : indexes) {
      outbound.add(transformOutboundIndex(index));
    }
    return outbound;
  }

  @Override
  public ColumnStatistics transformInboundColumnStatistics(ColumnStatistics columnStatistics) {
    ColumnStatistics inbound = new ColumnStatistics(columnStatistics);
    inbound.getStatsDesc().setDbName(
        metaStoreMapping.transformInboundDatabaseName(columnStatistics.getStatsDesc().getDbName()));
    return inbound;
  }

  @Override
  public ColumnStatistics transformOutboundColumnStatistics(ColumnStatistics columnStatistics) {
    ColumnStatistics outbound = new ColumnStatistics(columnStatistics);
    outbound.getStatsDesc().setDbName(
        metaStoreMapping.transformOutboundDatabaseName(columnStatistics.getStatsDesc().getDbName()));
    return outbound;
  }

  @Override
  public SetPartitionsStatsRequest transformInboundSetPartitionStatsRequest(SetPartitionsStatsRequest request) {
    SetPartitionsStatsRequest inbound = new SetPartitionsStatsRequest(request);
    if (request.isSetColStats()) {
      List<ColumnStatistics> inboundColumnStats = new ArrayList<>(request.getColStatsSize());
      for (ColumnStatistics stats : request.getColStats()) {
        inboundColumnStats.add(transformInboundColumnStatistics(stats));
      }
      inbound.setColStats(inboundColumnStats);
    }
    return inbound;
  }

  @Override
  public Function transformInboundFunction(Function function) {
    Function inbound = new Function(function);
    inbound.setDbName(metaStoreMapping.transformInboundDatabaseName(function.getDbName()));
    return inbound;
  }

  @Override
  public List<HiveObjectPrivilege> transformOutboundHiveObjectPrivileges(List<HiveObjectPrivilege> privileges) {
    List<HiveObjectPrivilege> outbound = new ArrayList<>(privileges.size());
    for (HiveObjectPrivilege privilege : privileges) {
      HiveObjectPrivilege outboundPriviledge = new HiveObjectPrivilege(privilege);
      outboundPriviledge.setHiveObject(transformOutboundHiveObjectRef(privilege.getHiveObject()));
      outbound.add(outboundPriviledge);
    }
    return outbound;
  }

  @Override
  public PrivilegeBag transformInboundPrivilegeBag(PrivilegeBag privilegeBag) {
    PrivilegeBag inbound = new PrivilegeBag(privilegeBag);
    if (privilegeBag.isSetPrivileges()) {
      List<HiveObjectPrivilege> privileges = new ArrayList<>(privilegeBag.getPrivilegesSize());
      for (HiveObjectPrivilege privilege : privilegeBag.getPrivileges()) {
        HiveObjectPrivilege inboundObjRef = new HiveObjectPrivilege(privilege);
        inboundObjRef.setHiveObject(transformInboundHiveObjectRef(privilege.getHiveObject()));
        privileges.add(inboundObjRef);
      }
      inbound.setPrivileges(privileges);
    }
    return inbound;
  }

  @Override
  public GrantRevokePrivilegeRequest transformInboundGrantRevokePrivilegesRequest(GrantRevokePrivilegeRequest request) {
    GrantRevokePrivilegeRequest inbound = new GrantRevokePrivilegeRequest(request);
    if (request.isSetPrivileges()) {
      inbound.setPrivileges(transformInboundPrivilegeBag(request.getPrivileges()));
    }
    return inbound;
  }

  @Override
  public LockRequest transformInboundLockRequest(LockRequest request) {
    LockRequest inbound = new LockRequest(request);
    if (request.isSetComponent()) {
      List<LockComponent> inboundComponents = new ArrayList<>(request.getComponentSize());
      for (LockComponent component : request.getComponent()) {
        LockComponent inboundComponent = new LockComponent(component);
        inboundComponent.setDbname(metaStoreMapping.transformInboundDatabaseName(component.getDbname()));
        inboundComponents.add(inboundComponent);
      }
      inbound.setComponent(inboundComponents);
    }
    return inbound;
  }

  @Override
  public CompactionRequest transformInboundCompactionRequest(CompactionRequest request) {
    CompactionRequest inbound = new CompactionRequest(request);
    inbound.setDbname(metaStoreMapping.transformInboundDatabaseName(request.getDbname()));
    return inbound;
  }

  @Override
  public void createDatabase(Database database)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    metaStoreMapping.createDatabase(database);
  }

  @Override
  public Database transformInboundDatabase(Database database) {
    Database outbound = new Database(database);
    outbound.setName(metaStoreMapping.transformInboundDatabaseName(database.getName()));
    return outbound;
  }

  @Override
  public List<PartitionSpec> transformInboundPartitionSpecs(List<PartitionSpec> partitionSpecs) {
    List<PartitionSpec> inbound = new ArrayList<>(partitionSpecs.size());
    for (PartitionSpec partitionSpec : partitionSpecs) {
      PartitionSpec inboundPartitionSpec = new PartitionSpec(partitionSpec);
      inboundPartitionSpec.setDbName(metaStoreMapping.transformInboundDatabaseName(partitionSpec.getDbName()));
      inbound.add(inboundPartitionSpec);
    }
    return inbound;
  }
}
