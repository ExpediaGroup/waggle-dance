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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class IdentityMappingTest {

  private final static String DB_NAME = "db";

  @Mock
  private MetaStoreMapping metastoreMapping;

  private IdentityMapping databaseMapping;
  private Database database;

  @Before
  public void setUp() {
    databaseMapping = new IdentityMapping(metastoreMapping);
    database = new Database();
    database.setName(DB_NAME);
  }

  @After
  public void after() {
    verify(metastoreMapping, never()).transformInboundDatabaseName(any(String.class));
    verify(metastoreMapping, never()).transformOutboundDatabaseName(any(String.class));
  }

  @Test
  public void transformOutboundTable() throws Exception {
    Table table = new Table();
    Table result = databaseMapping.transformOutboundTable(table);
    assertThat(result, is(sameInstance(table)));
  }

  @Test
  public void transformInboundTable() throws Exception {
    Table table = new Table();
    Table result = databaseMapping.transformInboundTable(table);
    assertThat(result, is(sameInstance(table)));
  }

  @Test
  public void transformOutboundPartition() throws Exception {
    Partition partition = new Partition();
    Partition result = databaseMapping.transformOutboundPartition(partition);
    assertThat(result, is(sameInstance(partition)));
  }

  @Test
  public void transformInboundPartition() throws Exception {
    Partition partition = new Partition();
    Partition result = databaseMapping.transformInboundPartition(partition);
    assertThat(result, is(sameInstance(partition)));
  }

  @Test
  public void transformOutboundIndex() throws Exception {
    Index index = new Index();
    Index result = databaseMapping.transformOutboundIndex(index);
    assertThat(result, is(sameInstance(index)));
  }

  @Test
  public void transformInboundIndex() throws Exception {
    Index index = new Index();
    Index result = databaseMapping.transformInboundIndex(index);
    assertThat(result, is(sameInstance(index)));
  }

  @Test
  public void transformOutboundFunction() throws Exception {
    Function function = new Function();
    Function result = databaseMapping.transformOutboundFunction(function);
    assertThat(result, is(sameInstance(function)));
  }

  @Test
  public void transformInboundHiveObjectRef() throws Exception {
    HiveObjectRef hiveObjectRef = new HiveObjectRef();
    HiveObjectRef result = databaseMapping.transformInboundHiveObjectRef(hiveObjectRef);
    assertThat(result, is(sameInstance(hiveObjectRef)));
  }

  @Test
  public void transformOutboundHiveObjectRef() throws Exception {
    HiveObjectRef hiveObjectRef = new HiveObjectRef();
    HiveObjectRef result = databaseMapping.transformOutboundHiveObjectRef(hiveObjectRef);
    assertThat(result, is(sameInstance(hiveObjectRef)));
  }

  @Test
  public void transformOutboundPartitionSpec() throws Exception {
    PartitionSpec partitionSpec = new PartitionSpec();
    PartitionSpec result = databaseMapping.transformOutboundPartitionSpec(partitionSpec);
    assertThat(result, is(sameInstance(partitionSpec)));
  }

  @Test
  public void transformInboundPartitionsStatsRequest() throws Exception {
    PartitionsStatsRequest partitionStatsRequest = new PartitionsStatsRequest();
    PartitionsStatsRequest result = databaseMapping.transformInboundPartitionsStatsRequest(partitionStatsRequest);
    assertThat(result, is(sameInstance(partitionStatsRequest)));
  }

  @Test
  public void transformInboundTableStatsRequest() throws Exception {
    TableStatsRequest tableStatsRequest = new TableStatsRequest();
    TableStatsRequest result = databaseMapping.transformInboundTableStatsRequest(tableStatsRequest);
    assertThat(result, is(sameInstance(tableStatsRequest)));
  }

  @Test
  public void transformInboundPartitionsByExprRequest() throws Exception {
    PartitionsByExprRequest partitionsByExprRequest = new PartitionsByExprRequest();
    PartitionsByExprRequest result = databaseMapping.transformInboundPartitionsByExprRequest(partitionsByExprRequest);
    assertThat(result, is(sameInstance(partitionsByExprRequest)));
  }

  @Test
  public void transformOutboundPartitionsByExprResult() throws Exception {
    PartitionsByExprResult partitionsByExprResult = new PartitionsByExprResult();
    PartitionsByExprResult result = databaseMapping.transformOutboundPartitionsByExprResult(partitionsByExprResult);
    assertThat(result, is(sameInstance(partitionsByExprResult)));
  }

  @Test
  public void getClient() throws Exception {
    databaseMapping.getClient();
    verify(metastoreMapping).getClient();
  }

  @Test
  public void transformOutboundDatabaseName() throws Exception {
    assertThat(databaseMapping.transformOutboundDatabaseName(DB_NAME), is(DB_NAME));
  }

  @Test
  public void transformInboundDatabaseName() throws Exception {
    assertThat(databaseMapping.transformInboundDatabaseName(DB_NAME), is(DB_NAME));
  }

  @Test
  public void transformOutboundDatabase() throws Exception {
    Database result = databaseMapping.transformOutboundDatabase(database);
    assertThat(result, is(sameInstance(database)));
  }

  @Test
  public void getDatabasePrefix() throws Exception {
    String result = databaseMapping.getDatabasePrefix();
    assertThat(result, is(IdentityMapping.EMPTY_PREFIX));
  }

  @Test
  public void getMetastoreMappingName() throws Exception {
    databaseMapping.getMetastoreMappingName();
    verify(metastoreMapping).getMetastoreMappingName();
  }

  @Test
  public void transformInboundCacheFileMetadataRequest() throws Exception {
    CacheFileMetadataRequest cacheFileMetadataRequest = new CacheFileMetadataRequest();
    CacheFileMetadataRequest result = databaseMapping
        .transformInboundCacheFileMetadataRequest(cacheFileMetadataRequest);
    assertThat(result, is(sameInstance(cacheFileMetadataRequest)));
  }

  @Test
  public void transformInboundFireEventRequest() throws Exception {
    FireEventRequest fireEventRequest = new FireEventRequest();
    FireEventRequest result = databaseMapping.transformInboundFireEventRequest(fireEventRequest);
    assertThat(result, is(sameInstance(fireEventRequest)));
  }

  @Test
  public void transformInboundForeignKeysRequest() throws Exception {
    ForeignKeysRequest foreignKeysRequest = new ForeignKeysRequest();
    ForeignKeysRequest result = databaseMapping.transformInboundForeignKeysRequest(foreignKeysRequest);
    assertThat(result, is(sameInstance(foreignKeysRequest)));
  }

  @Test
  public void transformInboundPrimaryKeysRequest() throws Exception {
    PrimaryKeysRequest primaryKeysRequest = new PrimaryKeysRequest();
    PrimaryKeysRequest result = databaseMapping.transformInboundPrimaryKeysRequest(primaryKeysRequest);
    assertThat(result, is(sameInstance(primaryKeysRequest)));
  }

  @Test
  public void transformOutboundTableMeta() throws Exception {
    TableMeta tableMeta = new TableMeta();
    TableMeta result = databaseMapping.transformOutboundTableMeta(tableMeta);
    assertThat(result, is(sameInstance(tableMeta)));
  }

  @Test
  public void transformInboundAddDynamicPartitions() throws Exception {
    AddDynamicPartitions addDynamicPartitions = new AddDynamicPartitions();
    AddDynamicPartitions result = databaseMapping.transformInboundAddDynamicPartitions(addDynamicPartitions);
    assertThat(result, is(sameInstance(addDynamicPartitions)));
  }

  @Test
  public void close() throws Exception {
    databaseMapping.close();
    verify(metastoreMapping).close();
  }

  @Test
  public void isAvailable() throws Exception {
    databaseMapping.isAvailable();
    verify(metastoreMapping).isAvailable();
  }

  @Test
  public void checkWritePermissions() throws Exception {
    databaseMapping.checkWritePermissions(DB_NAME);
    verify(metastoreMapping).checkWritePermissions(DB_NAME);
  }

  @Test
  public void transformInboundDropConstraintRequest() throws Exception {
    DropConstraintRequest dropConstraintRequest = new DropConstraintRequest();
    DropConstraintRequest result = databaseMapping.transformInboundDropConstraintRequest(dropConstraintRequest);
    assertThat(result, is(sameInstance(dropConstraintRequest)));
  }

  @Test
  public void transformInboundAddPartitionsRequest() throws Exception {
    AddPartitionsRequest addPartitionsRequest = new AddPartitionsRequest();
    AddPartitionsRequest result = databaseMapping.transformInboundAddPartitionsRequest(addPartitionsRequest);
    assertThat(result, is(sameInstance(addPartitionsRequest)));
  }

  @Test
  public void transformOutboundAddPartitionsResult() throws Exception {
    AddPartitionsResult addPartitionsResult = new AddPartitionsResult();
    AddPartitionsResult result = databaseMapping.transformOutboundAddPartitionsResult(addPartitionsResult);
    assertThat(result, is(sameInstance(addPartitionsResult)));
  }

  @Test
  public void transformInboundDropPartitionRequest() throws Exception {
    DropPartitionsRequest dropPartitionsRequest = new DropPartitionsRequest();
    DropPartitionsRequest result = databaseMapping.transformInboundDropPartitionRequest(dropPartitionsRequest);
    assertThat(result, is(sameInstance(dropPartitionsRequest)));
  }

  @Test
  public void transforOutboundDropPartitionsResult() throws Exception {
    DropPartitionsResult dropPartitionsResult = new DropPartitionsResult();
    DropPartitionsResult result = databaseMapping.transforOutboundDropPartitionsResult(dropPartitionsResult);
    assertThat(result, is(sameInstance(dropPartitionsResult)));
  }

  @Test
  public void transformOutboundPartitionSpecs() throws Exception {
    List<PartitionSpec> partitionSpecs = new ArrayList<>();
    List<PartitionSpec> result = databaseMapping.transformOutboundPartitionSpecs(partitionSpecs);
    assertThat(result, is(sameInstance(partitionSpecs)));
  }

  @Test
  public void transformOutboundIndexes() throws Exception {
    List<Index> indexes = new ArrayList<>();
    List<Index> result = databaseMapping.transformOutboundIndexes(indexes);
    assertThat(result, is(sameInstance(indexes)));
  }

  @Test
  public void transformInboundColumnStatistics() throws Exception {
    ColumnStatistics columnStatistics = new ColumnStatistics();
    ColumnStatistics result = databaseMapping.transformInboundColumnStatistics(columnStatistics);
    assertThat(result, is(sameInstance(columnStatistics)));
  }

  @Test
  public void transformOutboundColumnStatistics() throws Exception {
    ColumnStatistics columnStatistics = new ColumnStatistics();
    ColumnStatistics result = databaseMapping.transformOutboundColumnStatistics(columnStatistics);
    assertThat(result, is(sameInstance(columnStatistics)));
  }

  @Test
  public void transformInboundSetPartitionStatsRequest() throws Exception {
    SetPartitionsStatsRequest setPartitionsStatsRequest = new SetPartitionsStatsRequest();
    SetPartitionsStatsRequest result = databaseMapping
        .transformInboundSetPartitionStatsRequest(setPartitionsStatsRequest);
    assertThat(result, is(sameInstance(setPartitionsStatsRequest)));
  }

  @Test
  public void transformInboundSetPartitionStatsRequestNoColumnStats() throws Exception {
    SetPartitionsStatsRequest setPartitionsStatsRequest = new SetPartitionsStatsRequest();
    SetPartitionsStatsRequest result = databaseMapping
        .transformInboundSetPartitionStatsRequest(setPartitionsStatsRequest);
    assertThat(result, is(sameInstance(setPartitionsStatsRequest)));
  }

  @Test
  public void transformInboundFunction() throws Exception {
    Function function = new Function();
    Function result = databaseMapping.transformInboundFunction(function);
    assertThat(result, is(sameInstance(function)));
  }

  @Test
  public void transformOutboundHiveObjectPrivileges() throws Exception {
    List<HiveObjectPrivilege> hiveObjectPrivileges = new ArrayList<>();
    List<HiveObjectPrivilege> result = databaseMapping.transformOutboundHiveObjectPrivileges(hiveObjectPrivileges);
    assertThat(result, is(sameInstance(hiveObjectPrivileges)));
  }

  @Test
  public void transformInboundPrivilegeBag() throws Exception {
    PrivilegeBag privilegeBag = new PrivilegeBag();
    PrivilegeBag result = databaseMapping.transformInboundPrivilegeBag(privilegeBag);
    assertThat(result, is(sameInstance(privilegeBag)));
  }

  @Test
  public void transformInboundGrantRevokePrivilegesRequest() throws Exception {
    GrantRevokePrivilegeRequest grantRevokePrivilegeRequest = new GrantRevokePrivilegeRequest();
    GrantRevokePrivilegeRequest result = databaseMapping
        .transformInboundGrantRevokePrivilegesRequest(grantRevokePrivilegeRequest);
    assertThat(result, is(sameInstance(grantRevokePrivilegeRequest)));
  }

  @Test
  public void transformInboundLockRequest() throws Exception {
    LockRequest lockRequest = new LockRequest();
    LockRequest result = databaseMapping.transformInboundLockRequest(lockRequest);
    assertThat(result, is(sameInstance(lockRequest)));
  }

  @Test
  public void transformInboundCompactionRequest() throws Exception {
    CompactionRequest compactionRequest = new CompactionRequest();
    compactionRequest.setDbname(DB_NAME);
    CompactionRequest result = databaseMapping.transformInboundCompactionRequest(compactionRequest);
    assertThat(result, is(sameInstance(compactionRequest)));
  }

  @Test
  public void createDatabase() throws Exception {
    databaseMapping.createDatabase(database);
    verify(metastoreMapping).createDatabase(database);
  }

  @Test
  public void transformInboundDatabase() throws Exception {
    Database result = databaseMapping.transformInboundDatabase(database);
    assertThat(result, is(sameInstance(database)));
    assertThat(result.getName(), is(DB_NAME));
    // Should not be called
    verify(metastoreMapping, never()).transformOutboundDatabase(database);
  }

  @Test
  public void transformInboundPartitionSpecs() throws Exception {
    List<PartitionSpec> partitionSpecs = new ArrayList<>();
    List<PartitionSpec> result = databaseMapping.transformInboundPartitionSpecs(partitionSpecs);
    assertThat(result, is(sameInstance(partitionSpecs)));
  }

  @Test
  public void transformOutboundPartitions() throws Exception {
    List<Partition> partitions = new ArrayList<>();
    List<Partition> result = databaseMapping.transformOutboundPartitions(partitions);
    assertThat(result, is(sameInstance(partitions)));
  }

  @Test
  public void transformInboundPartitions() throws Exception {
    List<Partition> partitions = new ArrayList<>();
    List<Partition> result = databaseMapping.transformInboundPartitions(partitions);
    assertThat(result, is(sameInstance(partitions)));
  }

}
