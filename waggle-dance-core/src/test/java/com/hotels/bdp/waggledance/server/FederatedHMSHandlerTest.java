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
package com.hotels.bdp.waggledance.server;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.mapping.service.impl.NotifyingFederationService;

@RunWith(MockitoJUnitRunner.class)
public class FederatedHMSHandlerTest {

  private final static String DB_P = "db_primary";
  private final static String DB_S = "db_second";

  private @Mock MappingEventListener databaseMappingService;
  private @Mock NotifyingFederationService notifyingFederationService;
  private @Mock DatabaseMapping primaryMapping;
  private @Mock Iface primaryClient;

  private FederatedHMSHandler handler;

  @Before
  public void setUp() throws NoSuchObjectException {
    handler = new FederatedHMSHandler(databaseMappingService, notifyingFederationService);
    when(databaseMappingService.primaryDatabaseMapping()).thenReturn(primaryMapping);
    when(databaseMappingService.getDatabaseMappings()).thenReturn(Collections.singletonList(primaryMapping));
    when(primaryMapping.getClient()).thenReturn(primaryClient);
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn(DB_P);
    when(databaseMappingService.databaseMapping(DB_P)).thenReturn(primaryMapping);
  }

  @Test
  public void close() throws Exception {
    verify(notifyingFederationService).subscribe(databaseMappingService);
    handler.close();
    verify(notifyingFederationService).unsubscribe(databaseMappingService);
    verify(databaseMappingService).close();
  }

  @Test
  public void shutdown() throws Exception {
    verify(notifyingFederationService).subscribe(databaseMappingService);
    handler.shutdown();
    verify(notifyingFederationService).unsubscribe(databaseMappingService);
    verify(databaseMappingService).close();
  }

  @Test
  public void getMetaConf() throws Exception {
    handler.getMetaConf("key");
    verify(primaryClient).getMetaConf("key");
  }

  @Test
  public void setMetaConf() throws Exception {
    handler.setMetaConf("key", "value");
    verify(primaryClient).setMetaConf("key", "value");
  }

  @Test
  public void create_database() throws Exception {
    Database database = new Database();
    database.setName(DB_P);
    Database inboundDB = new Database();
    inboundDB.setName("inbound");
    when(primaryMapping.transformInboundDatabase(database)).thenReturn(inboundDB);
    handler.create_database(database);
    verify(primaryMapping).createDatabase(inboundDB);
  }

  @Test
  public void get_database() throws Exception {
    Database database = new Database();
    database.setName(DB_P);
    Database outboundDB = new Database();
    outboundDB.setName("outbound");
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_database("inbound")).thenReturn(database);
    when(primaryMapping.transformOutboundDatabase(database)).thenReturn(outboundDB);
    Database result = handler.get_database(DB_P);
    assertThat(result, is(outboundDB));
  }

  @Test
  public void drop_database() throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    handler.drop_database(DB_P, false, false);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).drop_database("inbound", false, false);
  }

  @Test
  public void get_databases() throws MetaException, TException {
    PanopticOperationHandler panopticHandler = Mockito.mock(PanopticOperationHandler.class);
    when(databaseMappingService.getPanopticOperationHandler()).thenReturn(panopticHandler);
    String pattern = "*";
    when(panopticHandler.getAllDatabases(pattern)).thenReturn(Lists.newArrayList(DB_P, DB_S));
    List<String> result = handler.get_databases(pattern);
    assertThat(result.size(), is(2));
    assertThat(result, contains(DB_P, DB_S));
  }

  @Test
  public void get_all_databases() throws MetaException, TException {
    PanopticOperationHandler panopticHandler = Mockito.mock(PanopticOperationHandler.class);
    when(databaseMappingService.getPanopticOperationHandler()).thenReturn(panopticHandler);
    when(panopticHandler.getAllDatabases()).thenReturn(Lists.newArrayList(DB_P, DB_S));
    List<String> result = handler.get_all_databases();
    assertThat(result.size(), is(2));
    assertThat(result, contains(DB_P, DB_S));
  }

  @Test
  public void alter_database() throws MetaException, NoSuchObjectException, TException {
    Database database = new Database();
    database.setName(DB_P);
    Database inboundDB = new Database();
    inboundDB.setName("inbound");
    when(primaryMapping.transformInboundDatabase(database)).thenReturn(inboundDB);
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");

    handler.alter_database(DB_P, database);
    verify(primaryMapping, times(2)).checkWritePermissions(DB_P);
    verify(primaryClient).alter_database("inbound", inboundDB);
  }

  @Test
  public void get_type() throws MetaException, NoSuchObjectException, TException {
    handler.get_type("name");
    verify(primaryClient).get_type("name");
  }

  @Test
  public void create_type() throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    Type type = new Type();
    handler.create_type(type);
    verify(primaryClient).create_type(type);
  }

  @Test
  public void drop_type() throws MetaException, NoSuchObjectException, TException {
    handler.drop_type("name");
    verify(primaryClient).drop_type("name");
  }

  @Test
  public void get_type_all() throws MetaException, TException {
    handler.get_type_all("name");
    verify(primaryClient).get_type_all("name");
  }

  @Test
  public void get_fields() throws MetaException, UnknownTableException, UnknownDBException, TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    handler.get_fields(DB_P, "table");
    verify(primaryClient).get_fields("inbound", "table");
  }

  @Test
  public void get_schema() throws MetaException, UnknownTableException, UnknownDBException, TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    handler.get_schema(DB_P, "table");
    verify(primaryClient).get_schema("inbound", "table");
  }

  @Test
  public void create_table()
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    Table table = new Table();
    table.setDbName(DB_P);
    Table inboundTable = new Table();
    inboundTable.setDbName("inbound");
    when(primaryMapping.transformInboundTable(table)).thenReturn(inboundTable);
    handler.create_table(table);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).create_table(inboundTable);
  }

  @Test
  public void create_table_with_environment_context()
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    Table table = new Table();
    table.setDbName(DB_P);
    Table inboundTable = new Table();
    inboundTable.setDbName("inbound");
    when(primaryMapping.transformInboundTable(table)).thenReturn(inboundTable);
    handler.create_table_with_environment_context(table, environmentContext);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).create_table_with_environment_context(inboundTable, environmentContext);
  }

  @Test
  public void drop_table() throws NoSuchObjectException, MetaException, TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    handler.drop_table(DB_P, "table", false);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).drop_table("inbound", "table", false);
  }

  @Test
  public void drop_table_with_environment_context() throws NoSuchObjectException, MetaException, TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    handler.drop_table_with_environment_context(DB_P, "table", false, environmentContext);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).drop_table_with_environment_context("inbound", "table", false, environmentContext);
  }

  @Test
  public void get_tables() throws MetaException, TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    List<String> tables = Lists.newArrayList("table1");
    when(primaryClient.get_tables("inbound", "*")).thenReturn(tables);
    List<String> result = handler.get_tables(DB_P, "*");
    assertThat(result, is(tables));
  }

  @Test
  public void get_all_tables() throws MetaException, TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    List<String> tables = Lists.newArrayList("table1");
    when(primaryClient.get_all_tables("inbound")).thenReturn(tables);
    List<String> result = handler.get_all_tables(DB_P);
    assertThat(result, is(tables));
  }

  @Test
  public void get_table() throws MetaException, NoSuchObjectException, TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    Table table = new Table();
    Table outbound = new Table();
    when(primaryClient.get_table("inbound", "table")).thenReturn(table);
    when(primaryMapping.transformOutboundTable(table)).thenReturn(outbound);
    Table result = handler.get_table(DB_P, "table");
    assertThat(result, is(outbound));
  }

  @Test
  public void get_table_objects_by_name()
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    Table table = new Table();
    Table outbound = new Table();
    when(primaryClient.get_table_objects_by_name("inbound", Lists.newArrayList("table")))
        .thenReturn(Lists.newArrayList(table));
    when(primaryMapping.transformOutboundTable(table)).thenReturn(outbound);
    List<Table> result = handler.get_table_objects_by_name(DB_P, Lists.newArrayList("table"));
    List<Table> expected = Lists.newArrayList(outbound);
    assertThat(result, is(expected));
  }

  @Test
  public void get_table_names_by_filter()
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    List<String> tables = Lists.newArrayList("table1");
    when(primaryClient.get_table_names_by_filter("inbound", "*", (short) 2)).thenReturn(tables);
    List<String> result = handler.get_table_names_by_filter(DB_P, "*", (short) 2);
    assertThat(result, is(tables));
  }

  @Test
  public void alter_table() throws InvalidOperationException, MetaException, TException {
    Table table = new Table();
    table.setDbName(DB_P);
    Table inbound = new Table();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryMapping.transformInboundTable(table)).thenReturn(inbound);
    handler.alter_table(DB_P, "table", table);
    verify(primaryMapping, times(2)).checkWritePermissions(DB_P);
    verify(primaryClient).alter_table("inbound", "table", inbound);
  }

  @Test
  public void alter_table_with_environment_context() throws InvalidOperationException, MetaException, TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    Table table = new Table();
    table.setDbName(DB_P);
    Table inbound = new Table();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryMapping.transformInboundTable(table)).thenReturn(inbound);
    handler.alter_table_with_environment_context(DB_P, "table", table, environmentContext);
    verify(primaryMapping, times(2)).checkWritePermissions(DB_P);
    verify(primaryClient).alter_table_with_environment_context("inbound", "table", inbound, environmentContext);
  }

  @Test
  public void add_partition() throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    Partition newPartition = new Partition();
    newPartition.setDbName(DB_P);
    Partition inbound = new Partition();
    Partition outbound = new Partition();
    when(primaryMapping.transformInboundPartition(newPartition)).thenReturn(inbound);
    when(primaryClient.add_partition(inbound)).thenReturn(inbound);
    when(primaryMapping.transformOutboundPartition(inbound)).thenReturn(outbound);
    Partition result = handler.add_partition(newPartition);
    assertThat(result, is(outbound));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void add_partition_with_environment_context()
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    Partition newPartition = new Partition();
    newPartition.setDbName(DB_P);
    Partition inbound = new Partition();
    Partition outbound = new Partition();
    when(primaryMapping.transformInboundPartition(newPartition)).thenReturn(inbound);
    when(primaryClient.add_partition_with_environment_context(inbound, environmentContext)).thenReturn(inbound);
    when(primaryMapping.transformOutboundPartition(inbound)).thenReturn(outbound);
    Partition result = handler.add_partition_with_environment_context(newPartition, environmentContext);
    assertThat(result, is(outbound));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void add_partitions() throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    Partition newPartition1 = new Partition();
    newPartition1.setDbName(DB_P);
    Partition newPartition2 = new Partition();
    newPartition2.setDbName(DB_P);
    List<Partition> inbound = Lists.newArrayList(new Partition());
    List<Partition> partitions = Lists.newArrayList(newPartition1, newPartition2);
    when(primaryMapping.transformInboundPartitions(partitions)).thenReturn(inbound);
    when(primaryClient.add_partitions(inbound)).thenReturn(2);
    int result = handler.add_partitions(partitions);
    assertThat(result, is(2));
    verify(primaryMapping, times(2)).checkWritePermissions(DB_P);
  }

  @Test
  public void add_partitions_pspec() throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    PartitionSpec newPartitionPSpec1 = new PartitionSpec();
    newPartitionPSpec1.setDbName(DB_P);
    PartitionSpec newPartitionPspec2 = new PartitionSpec();
    newPartitionPspec2.setDbName(DB_P);
    List<PartitionSpec> inbound = Lists.newArrayList(new PartitionSpec());
    List<PartitionSpec> partitionsPspec = Lists.newArrayList(newPartitionPSpec1, newPartitionPspec2);
    when(primaryMapping.transformInboundPartitionSpecs(partitionsPspec)).thenReturn(inbound);
    when(primaryClient.add_partitions_pspec(inbound)).thenReturn(2);
    int result = handler.add_partitions_pspec(partitionsPspec);
    assertThat(result, is(2));
    verify(primaryMapping, times(2)).checkWritePermissions(DB_P);
  }

  @Test
  public void append_partition() throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    Partition inbound = new Partition();
    Partition outbound = new Partition();
    List<String> partVals = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.append_partition("inbound", "table1", partVals)).thenReturn(inbound);
    when(primaryMapping.transformOutboundPartition(inbound)).thenReturn(outbound);
    Partition result = handler.append_partition(DB_P, "table1", partVals);
    assertThat(result, is(outbound));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void add_partitions_req() throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    Partition newPartition1 = new Partition();
    newPartition1.setDbName(DB_P);
    Partition newPartition2 = new Partition();
    newPartition2.setDbName(DB_P);
    List<Partition> partitions = Lists.newArrayList(newPartition1, newPartition2);
    AddPartitionsRequest request = new AddPartitionsRequest();
    request.setDbName(DB_P);
    request.setParts(partitions);
    AddPartitionsRequest inbound = new AddPartitionsRequest();
    AddPartitionsResult addPartitionResult = new AddPartitionsResult();
    AddPartitionsResult outbound = new AddPartitionsResult();
    when(primaryMapping.transformInboundAddPartitionsRequest(request)).thenReturn(inbound);
    when(primaryClient.add_partitions_req(inbound)).thenReturn(addPartitionResult);
    when(primaryMapping.transformOutboundAddPartitionsResult(addPartitionResult)).thenReturn(outbound);

    AddPartitionsResult result = handler.add_partitions_req(request);
    assertThat(result, is(outbound));
    verify(primaryMapping, times(3)).checkWritePermissions(DB_P);
  }

  @Test
  public void append_partition_with_environment_context()
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    Partition inbound = new Partition();
    Partition outbound = new Partition();
    List<String> partVals = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.append_partition_with_environment_context("inbound", "table1", partVals, environmentContext))
        .thenReturn(inbound);
    when(primaryMapping.transformOutboundPartition(inbound)).thenReturn(outbound);
    Partition result = handler.append_partition_with_environment_context(DB_P, "table1", partVals, environmentContext);
    assertThat(result, is(outbound));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void append_partition_by_name()
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    Partition inbound = new Partition();
    Partition outbound = new Partition();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.append_partition_by_name("inbound", "table1", "partName")).thenReturn(inbound);
    when(primaryMapping.transformOutboundPartition(inbound)).thenReturn(outbound);
    Partition result = handler.append_partition_by_name(DB_P, "table1", "partName");
    assertThat(result, is(outbound));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void append_partition_by_name_with_environment_context()
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    Partition inbound = new Partition();
    Partition outbound = new Partition();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient
        .append_partition_by_name_with_environment_context("inbound", "table1", "partName", environmentContext))
            .thenReturn(inbound);
    when(primaryMapping.transformOutboundPartition(inbound)).thenReturn(outbound);
    Partition result = handler
        .append_partition_by_name_with_environment_context(DB_P, "table1", "partName", environmentContext);
    assertThat(result, is(outbound));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void drop_partition() throws NoSuchObjectException, MetaException, TException {
    List<String> partVals = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.drop_partition("inbound", "table1", partVals, false)).thenReturn(true);
    boolean result = handler.drop_partition(DB_P, "table1", partVals, false);
    assertThat(result, is(true));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void drop_partition_with_environment_context() throws NoSuchObjectException, MetaException, TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    List<String> partVals = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(
        primaryClient.drop_partition_with_environment_context("inbound", "table1", partVals, false, environmentContext))
            .thenReturn(true);
    boolean result = handler
        .drop_partition_with_environment_context(DB_P, "table1", partVals, false, environmentContext);
    assertThat(result, is(true));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void drop_partition_by_name() throws NoSuchObjectException, MetaException, TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.drop_partition_by_name("inbound", "table1", "partName", false)).thenReturn(true);
    boolean result = handler.drop_partition_by_name(DB_P, "table1", "partName", false);
    assertThat(result, is(true));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void drop_partition_by_name_with_environment_context()
    throws NoSuchObjectException, MetaException, TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient
        .drop_partition_by_name_with_environment_context("inbound", "table1", "partName", false, environmentContext))
            .thenReturn(true);
    boolean result = handler
        .drop_partition_by_name_with_environment_context(DB_P, "table1", "partName", false, environmentContext);
    assertThat(result, is(true));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void drop_partitions_req() throws NoSuchObjectException, MetaException, TException {
    DropPartitionsRequest req = new DropPartitionsRequest();
    req.setDbName(DB_P);
    DropPartitionsRequest inbound = new DropPartitionsRequest();
    DropPartitionsResult dropPartitionResult = new DropPartitionsResult();
    DropPartitionsResult outbound = new DropPartitionsResult();
    when(primaryMapping.transformInboundDropPartitionRequest(req)).thenReturn(inbound);
    when(primaryClient.drop_partitions_req(inbound)).thenReturn(dropPartitionResult);
    when(primaryMapping.transformOutboundDropPartitionsResult(dropPartitionResult)).thenReturn(outbound);
    DropPartitionsResult result = handler.drop_partitions_req(req);
    assertThat(result, is(outbound));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partition() throws MetaException, NoSuchObjectException, TException {
    List<String> partVals = Lists.newArrayList();
    Partition partition = new Partition();
    Partition outbound = new Partition();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_partition("inbound", "table1", partVals)).thenReturn(partition);
    when(primaryMapping.transformOutboundPartition(partition)).thenReturn(outbound);
    Partition result = handler.get_partition(DB_P, "table1", partVals);
    assertThat(result, is(outbound));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void exchange_partition()
    throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    Partition partition = new Partition();
    Partition outbound = new Partition();
    Map<String, String> specs = new HashMap<>();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.exchange_partition(specs, "inbound", "soureTable", "inbound", "destTable"))
        .thenReturn(partition);
    when(primaryMapping.transformOutboundPartition(partition)).thenReturn(outbound);
    Partition result = handler.exchange_partition(specs, DB_P, "soureTable", DB_P, "destTable");
    assertThat(result, is(outbound));
    verify(primaryMapping, times(2)).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partition_with_auth() throws MetaException, NoSuchObjectException, TException {
    List<String> partVals = Lists.newArrayList();
    Partition partition = new Partition();
    Partition outbound = new Partition();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    List<String> groupNames = new ArrayList<>();
    when(primaryClient.get_partition_with_auth("inbound", "table1", partVals, "user", groupNames))
        .thenReturn(partition);
    when(primaryMapping.transformOutboundPartition(partition)).thenReturn(outbound);
    Partition result = handler.get_partition_with_auth(DB_P, "table1", partVals, "user", groupNames);
    assertThat(result, is(outbound));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partition_by_name() throws MetaException, NoSuchObjectException, TException {
    Partition partition = new Partition();
    Partition outbound = new Partition();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_partition_by_name("inbound", "table1", "partName")).thenReturn(partition);
    when(primaryMapping.transformOutboundPartition(partition)).thenReturn(outbound);
    Partition result = handler.get_partition_by_name(DB_P, "table1", "partName");
    assertThat(result, is(outbound));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partitions() throws NoSuchObjectException, MetaException, TException {
    List<Partition> partitions = Lists.newArrayList();
    List<Partition> outbound = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_partitions("inbound", "table", (short) 10)).thenReturn(partitions);
    when(primaryMapping.transformOutboundPartitions(partitions)).thenReturn(outbound);
    List<Partition> result = handler.get_partitions(DB_P, "table", (short) 10);
    assertThat(result, is(outbound));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partitions_with_auth() throws NoSuchObjectException, MetaException, TException {
    List<Partition> partitions = Lists.newArrayList();
    List<Partition> outbound = Lists.newArrayList();
    List<String> groupNames = new ArrayList<>();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_partitions_with_auth("inbound", "table", (short) 10, "user", groupNames))
        .thenReturn(partitions);
    when(primaryMapping.transformOutboundPartitions(partitions)).thenReturn(outbound);
    List<Partition> result = handler.get_partitions_with_auth(DB_P, "table", (short) 10, "user", groupNames);
    assertThat(result, is(outbound));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partitions_pspec() throws NoSuchObjectException, MetaException, TException {
    List<PartitionSpec> partitionSpecs = Lists.newArrayList();
    List<PartitionSpec> outbound = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_partitions_pspec("inbound", "table", (short) 10)).thenReturn(partitionSpecs);
    when(primaryMapping.transformOutboundPartitionSpecs(partitionSpecs)).thenReturn(outbound);
    List<PartitionSpec> result = handler.get_partitions_pspec(DB_P, "table", (short) 10);
    assertThat(result, is(outbound));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partition_names() throws MetaException, TException {
    List<String> partitions = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_partition_names("inbound", "table", (short) 10)).thenReturn(partitions);
    List<String> result = handler.get_partition_names(DB_P, "table", (short) 10);
    assertThat(result, is(partitions));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partitions_ps() throws NoSuchObjectException, MetaException, TException {
    List<Partition> partitions = Lists.newArrayList();
    List<Partition> outbound = Lists.newArrayList();
    List<String> partVals = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_partitions_ps("inbound", "table", partVals, (short) 10)).thenReturn(partitions);
    when(primaryMapping.transformOutboundPartitions(partitions)).thenReturn(outbound);
    List<Partition> result = handler.get_partitions_ps(DB_P, "table", partVals, (short) 10);
    assertThat(result, is(outbound));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partitions_ps_with_auth() throws NoSuchObjectException, MetaException, TException {
    List<Partition> partitions = Lists.newArrayList();
    List<Partition> outbound = Lists.newArrayList();
    List<String> partVals = Lists.newArrayList();
    List<String> groupNames = new ArrayList<>();

    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_partitions_ps_with_auth("inbound", "table", partVals, (short) 10, "user", groupNames))
        .thenReturn(partitions);
    when(primaryMapping.transformOutboundPartitions(partitions)).thenReturn(outbound);
    List<Partition> result = handler
        .get_partitions_ps_with_auth(DB_P, "table", partVals, (short) 10, "user", groupNames);
    assertThat(result, is(outbound));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partition_names_ps() throws NoSuchObjectException, MetaException, TException {
    List<String> partitions = Lists.newArrayList();
    List<String> outbound = Lists.newArrayList();
    List<String> partVals = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_partition_names_ps("inbound", "table", partVals, (short) 10)).thenReturn(partitions);
    List<String> result = handler.get_partition_names_ps(DB_P, "table", partVals, (short) 10);
    assertThat(result, is(outbound));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partitions_by_filter() throws MetaException, NoSuchObjectException, TException {
    List<Partition> partitions = Lists.newArrayList();
    List<Partition> outbound = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_partitions_by_filter("inbound", "table", "*", (short) 10)).thenReturn(partitions);
    when(primaryMapping.transformOutboundPartitions(partitions)).thenReturn(outbound);
    List<Partition> result = handler.get_partitions_by_filter(DB_P, "table", "*", (short) 10);
    assertThat(result, is(outbound));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void get_part_specs_by_filter() throws MetaException, NoSuchObjectException, TException {
    List<PartitionSpec> partitionSpecs = Lists.newArrayList();
    List<PartitionSpec> outbound = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_part_specs_by_filter("inbound", "table", "*", (short) 10)).thenReturn(partitionSpecs);
    when(primaryMapping.transformOutboundPartitionSpecs(partitionSpecs)).thenReturn(outbound);
    List<PartitionSpec> result = handler.get_part_specs_by_filter(DB_P, "table", "*", (short) 10);
    assertThat(result, is(outbound));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partitions_by_expr() throws MetaException, NoSuchObjectException, TException {
    PartitionsByExprRequest req = new PartitionsByExprRequest();
    req.setDbName(DB_P);
    PartitionsByExprRequest inbound = new PartitionsByExprRequest();
    PartitionsByExprResult partitionResult = new PartitionsByExprResult();
    PartitionsByExprResult outbound = new PartitionsByExprResult();
    when(primaryMapping.transformInboundPartitionsByExprRequest(req)).thenReturn(inbound);
    when(primaryClient.get_partitions_by_expr(inbound)).thenReturn(partitionResult);
    when(primaryMapping.transformOutboundPartitionsByExprResult(partitionResult)).thenReturn(outbound);
    PartitionsByExprResult result = handler.get_partitions_by_expr(req);
    assertThat(result, is(outbound));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partitions_by_names() throws MetaException, NoSuchObjectException, TException {
    List<Partition> partitions = Lists.newArrayList();
    List<Partition> outbound = Lists.newArrayList();
    List<String> names = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_partitions_by_names("inbound", "table", names)).thenReturn(partitions);
    when(primaryMapping.transformOutboundPartitions(partitions)).thenReturn(outbound);
    List<Partition> result = handler.get_partitions_by_names(DB_P, "table", names);
    assertThat(result, is(outbound));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void flushCache() throws TException {
    handler.flushCache();
    verify(primaryClient).flushCache();
  }

  @Test
  public void get_all_functions() throws TException {
    PanopticOperationHandler panopticHandler = Mockito.mock(PanopticOperationHandler.class);
    when(databaseMappingService.getPanopticOperationHandler()).thenReturn(panopticHandler);
    DatabaseMapping mapping = Mockito.mock(DatabaseMapping.class);
    List<DatabaseMapping> mappings = Lists.newArrayList(mapping);
    when(databaseMappingService.getDatabaseMappings()).thenReturn(mappings);
    GetAllFunctionsResponse getAllFunctionsResponse = Mockito.mock(GetAllFunctionsResponse.class);
    when(panopticHandler.getAllFunctions(mappings)).thenReturn(getAllFunctionsResponse);
    GetAllFunctionsResponse result = handler.get_all_functions();
    assertThat(result, is(getAllFunctionsResponse));
  }

  @Test
  public void set_ugi() throws MetaException, TException {
    PanopticOperationHandler panopticHandler = Mockito.mock(PanopticOperationHandler.class);
    when(databaseMappingService.getPanopticOperationHandler()).thenReturn(panopticHandler);
    String user_name = "user";
    List<String> group_names = Lists.newArrayList("group");
    when(panopticHandler.setUgi(user_name, group_names, Collections.singletonList(primaryMapping)))
        .thenReturn(Lists.newArrayList("returned"));
    List<String> result = handler.set_ugi(user_name, group_names);
    assertThat(result.size(), is(1));
    assertThat(result, contains("returned"));
  }

  // Hive 2.3.0 methods
  @Test
  public void get_tables_by_type() throws MetaException, TException {
    when(primaryClient.get_tables_by_type(DB_P, "tbl*", "EXTERNAL_TABLE")).thenReturn(Arrays.asList("tbl0", "tbl1"));
    List<String> tables = handler.get_tables_by_type(DB_P, "tbl*", TableType.EXTERNAL_TABLE.name());
    verify(primaryClient).get_tables_by_type(DB_P, "tbl*", "EXTERNAL_TABLE");
    assertThat(tables.size(), is(2));
    assertThat(tables.get(0), is("tbl0"));
    assertThat(tables.get(1), is("tbl1"));
  }

  @Test
  public void get_table_req() throws MetaException, NoSuchObjectException, TException {
    Table table = new Table();
    table.setDbName(DB_P);
    table.setTableName("table");
    GetTableRequest request = new GetTableRequest(table.getDbName(), table.getTableName());
    GetTableResult response = new GetTableResult(table);
    when(primaryClient.get_table_req(request)).thenReturn(response);
    when(primaryMapping.transformInboundGetTableRequest(request)).thenReturn(request);
    when(primaryMapping.transformOutboundGetTableResult(response)).thenReturn(response);
    GetTableResult result = handler.get_table_req(request);
    assertThat(result.getTable().getDbName(), is(DB_P));
    assertThat(result.getTable().getTableName(), is("table"));
  }

  @Test
  public void get_table_objects_by_name_req()
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    Table table0 = new Table();
    table0.setDbName(DB_P);
    table0.setTableName("table0");
    Table table1 = new Table();
    table1.setDbName(DB_P);
    table1.setTableName("table1");
    GetTablesRequest request = new GetTablesRequest(DB_P);
    request.setTblNames(Arrays.asList(table0.getTableName(), table1.getTableName()));
    GetTablesResult response = new GetTablesResult(Arrays.asList(table0, table1));
    when(primaryClient.get_table_objects_by_name_req(request)).thenReturn(response);
    when(primaryMapping.transformInboundGetTablesRequest(request)).thenReturn(request);
    when(primaryMapping.transformOutboundGetTablesResult(response)).thenReturn(response);
    GetTablesResult result = handler.get_table_objects_by_name_req(request);
    assertThat(result.getTables().size(), is(2));
    assertThat(result.getTables().get(0).getDbName(), is(DB_P));
    assertThat(result.getTables().get(0).getTableName(), is("table0"));
    assertThat(result.getTables().get(1).getDbName(), is(DB_P));
    assertThat(result.getTables().get(1).getTableName(), is("table1"));
  }

  @Test
  public void get_foreign_keys() throws MetaException, InvalidOperationException, UnknownDBException, TException {
    ForeignKeysRequest request = new ForeignKeysRequest();
    request.setParent_db_name(null);
    request.setParent_tbl_name(null);
    request.setForeign_db_name(DB_S);
    request.setForeign_tbl_name("table");
    SQLForeignKey key = new SQLForeignKey();
    key.setFktable_db(DB_S);
    key.setFktable_name("table");
    ForeignKeysResponse response = new ForeignKeysResponse(Arrays.asList(key));

    when(databaseMappingService.databaseMapping(request.getForeign_db_name())).thenReturn(primaryMapping);
    when(primaryMapping.transformInboundForeignKeysRequest(request)).thenReturn(request);
    when(primaryClient.get_foreign_keys(request)).thenReturn(response);
    response.getForeignKeys().get(0).setFktable_db(DB_P);
    when(primaryMapping.transformOutboundForeignKeysResponse(response)).thenReturn(response);

    ForeignKeysResponse result = handler.get_foreign_keys(request);
    assertThat(result.getForeignKeys().get(0).getFktable_db(), is(DB_P));
    assertThat(result.getForeignKeys().get(0).getFktable_name(), is("table"));
  }

  @Test
  public void compact2() throws TException {
    CompactionRequest request = new CompactionRequest(DB_P, "table", CompactionType.MAJOR);
    CompactionResponse response = new CompactionResponse(0, "state", true);
    when(primaryClient.compact2(request)).thenReturn(response);
    when(primaryMapping.transformInboundCompactionRequest(request)).thenReturn(request);
    CompactionResponse result = handler.compact2(request);
    assertThat(result, is(response));
  }

  @Test
  public void getPrivilegeSet() throws Exception {
    String userName = "user";
    List<String> groupNames = Lists.newArrayList("group");
    HiveObjectRef hiveObjectRef = new HiveObjectRef();
    hiveObjectRef.setDbName(DB_P);
    when(databaseMappingService.databaseMapping(DB_P)).thenReturn(primaryMapping);
    when(primaryMapping.transformInboundHiveObjectRef(hiveObjectRef)).thenReturn(hiveObjectRef);
    PrincipalPrivilegeSet principalPrivilegeSet = new PrincipalPrivilegeSet();
    when(primaryClient.get_privilege_set(hiveObjectRef, userName, groupNames)).thenReturn(principalPrivilegeSet);
    PrincipalPrivilegeSet result = handler.get_privilege_set(hiveObjectRef, userName, groupNames);
    assertThat(result, is(principalPrivilegeSet));
  }

  @Test
  public void getPrivilegeSetDbNameIsNullShouldUsePrimary() throws Exception {
    String userName = "user";
    List<String> groupNames = Lists.newArrayList("group");
    HiveObjectRef hiveObjectRef = new HiveObjectRef();
    hiveObjectRef.setDbName(null);
    when(primaryMapping.transformInboundHiveObjectRef(hiveObjectRef)).thenReturn(hiveObjectRef);
    PrincipalPrivilegeSet principalPrivilegeSet = new PrincipalPrivilegeSet();
    when(primaryClient.get_privilege_set(hiveObjectRef, userName, groupNames)).thenReturn(principalPrivilegeSet);
    PrincipalPrivilegeSet result = handler.get_privilege_set(hiveObjectRef, userName, groupNames);
    assertThat(result, is(principalPrivilegeSet));
    verify(databaseMappingService, never()).databaseMapping(DB_P);
  }

}
