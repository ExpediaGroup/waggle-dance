/**
 * Copyright (C) 2016-2024 Expedia, Inc.
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

import static org.apache.hadoop.hive.metastore.api.CmRecycleRequest._Fields.DATA_PATH;
import static org.apache.hadoop.hive.metastore.api.GetRuntimeStatsRequest._Fields.MAX_WEIGHT;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddCheckConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDefaultConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AddForeignKeyRequest;
import org.apache.hadoop.hive.metastore.api.AddNotNullConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AddPrimaryKeyRequest;
import org.apache.hadoop.hive.metastore.api.AddUniqueConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.AlterCatalogRequest;
import org.apache.hadoop.hive.metastore.api.AlterISchemaRequest;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.CreateCatalogRequest;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.DropCatalogRequest;
import org.apache.hadoop.hive.metastore.api.DropConstraintRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetCatalogRequest;
import org.apache.hadoop.hive.metastore.api.GetCatalogResponse;
import org.apache.hadoop.hive.metastore.api.GetCatalogsResponse;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataByExprRequest;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataByExprResult;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.GetFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRuntimeStatsRequest;
import org.apache.hadoop.hive.metastore.api.GetSerdeRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokeType;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockLevel;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockType;
import org.apache.hadoop.hive.metastore.api.MapSchemaVersionToSerdeRequest;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRow;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsResult;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.SetSchemaVersionStateRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.TableStatsResult;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterPoolRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterPoolResponse;
import org.apache.hadoop.hive.metastore.api.WMAlterResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMAlterTriggerRequest;
import org.apache.hadoop.hive.metastore.api.WMAlterTriggerResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateOrDropTriggerToPoolMappingRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateOrDropTriggerToPoolMappingResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateOrUpdateMappingRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateOrUpdateMappingResponse;
import org.apache.hadoop.hive.metastore.api.WMCreatePoolRequest;
import org.apache.hadoop.hive.metastore.api.WMCreatePoolResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMCreateTriggerRequest;
import org.apache.hadoop.hive.metastore.api.WMCreateTriggerResponse;
import org.apache.hadoop.hive.metastore.api.WMDropMappingRequest;
import org.apache.hadoop.hive.metastore.api.WMDropMappingResponse;
import org.apache.hadoop.hive.metastore.api.WMDropPoolRequest;
import org.apache.hadoop.hive.metastore.api.WMDropPoolResponse;
import org.apache.hadoop.hive.metastore.api.WMDropResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMDropResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMDropTriggerRequest;
import org.apache.hadoop.hive.metastore.api.WMDropTriggerResponse;
import org.apache.hadoop.hive.metastore.api.WMGetActiveResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetActiveResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMGetAllResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetAllResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMGetResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMGetTriggersForResourePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMGetTriggersForResourePlanResponse;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanRequest;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.facebook.fb303.fb_status;
import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.mapping.service.impl.NotifyingFederationService;

@RunWith(MockitoJUnitRunner.class)
public class FederatedHMSHandlerTest {

  private final static String DB_P = "db_primary";
  private final static String DB_S = "db_second";
  private final static String TBL_1 = "table1";
  private final static String TBL_2 = "table2";
  private final static String CAT_1 = "cat1";
  private final static String CAT_2 = "cat2";
  private final static String SCH_1 = "sch1";

  private @Mock MappingEventListener databaseMappingService;
  private @Mock NotifyingFederationService notifyingFederationService;
  private @Mock DatabaseMapping primaryMapping;
  private @Mock Iface primaryClient;
  private @Mock WaggleDanceConfiguration waggleDanceConfiguration;
  private @Mock SaslServerWrapper saslServerWrapper;
  private @Mock MetastoreDelegationTokenManager metastoreDelegationTokenManager;

  private FederatedHMSHandler handler;

  @Before
  public void setUp() throws NoSuchObjectException {
    handler = new FederatedHMSHandler(databaseMappingService, notifyingFederationService,
        waggleDanceConfiguration, saslServerWrapper);
    when(databaseMappingService.primaryDatabaseMapping()).thenReturn(primaryMapping);
    when(databaseMappingService.getAvailableDatabaseMappings()).thenReturn(Collections.singletonList(primaryMapping));
    when(primaryMapping.getClient()).thenReturn(primaryClient);
    when(primaryMapping.getMetastoreFilter()).thenReturn(new DefaultMetaStoreFilterHookImpl(new HiveConf()));
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
  public void drop_database() throws TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    handler.drop_database(DB_P, false, false);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).drop_database("inbound", false, false);
  }

  @Test
  public void get_databases() throws TException {
    PanopticOperationHandler panopticHandler = Mockito.mock(PanopticOperationHandler.class);
    when(databaseMappingService.getPanopticOperationHandler()).thenReturn(panopticHandler);
    String pattern = "*";
    when(panopticHandler.getAllDatabases(pattern)).thenReturn(Lists.newArrayList(DB_P, DB_S));
    List<String> result = handler.get_databases(pattern);
    assertThat(result.size(), is(2));
    assertThat(result, contains(DB_P, DB_S));
  }

  @Test
  public void get_all_databases() throws TException {
    PanopticOperationHandler panopticHandler = Mockito.mock(PanopticOperationHandler.class);
    when(databaseMappingService.getPanopticOperationHandler()).thenReturn(panopticHandler);
    when(panopticHandler.getAllDatabases()).thenReturn(Lists.newArrayList(DB_P, DB_S));
    List<String> result = handler.get_all_databases();
    assertThat(result.size(), is(2));
    assertThat(result, contains(DB_P, DB_S));
  }

  @Test
  public void alter_database() throws TException {
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
  public void get_type() throws TException {
    handler.get_type("name");
    verify(primaryClient).get_type("name");
  }

  @Test
  public void create_type() throws TException {
    Type type = new Type();
    handler.create_type(type);
    verify(primaryClient).create_type(type);
  }

  @Test
  public void drop_type() throws TException {
    handler.drop_type("name");
    verify(primaryClient).drop_type("name");
  }

  @Test
  public void get_type_all() throws TException {
    handler.get_type_all("name");
    verify(primaryClient).get_type_all("name");
  }

  @Test
  public void get_fields() throws TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    handler.get_fields(DB_P, "table");
    verify(primaryClient).get_fields("inbound", "table");
  }

  @Test
  public void get_schema() throws TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    handler.get_schema(DB_P, "table");
    verify(primaryClient).get_schema("inbound", "table");
  }

  @Test
  public void create_table() throws TException {
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
  public void create_table_with_environment_context() throws TException {
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
  public void drop_table() throws TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    handler.drop_table(DB_P, "table", false);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).drop_table("inbound", "table", false);
  }

  @Test
  public void drop_table_with_environment_context() throws TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    handler.drop_table_with_environment_context(DB_P, "table", false, environmentContext);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).drop_table_with_environment_context("inbound", "table", false, environmentContext);
  }

  @Test
  public void get_tables() throws TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    List<String> tables = Lists.newArrayList("table1");
    when(primaryClient.get_tables("inbound", "*")).thenReturn(tables);
    when(databaseMappingService.filterTables(DB_P, tables, primaryMapping)).thenReturn(tables);
    List<String> result = handler.get_tables(DB_P, "*");
    assertThat(result, is(tables));
  }

  @Test
  public void get_all_tables() throws TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    List<String> tables = Lists.newArrayList("table1", "table2");
    List<String> filteredTables = Lists.newArrayList("table2");
    when(primaryClient.get_all_tables("inbound")).thenReturn(tables);
    when(databaseMappingService.filterTables(DB_P, tables, primaryMapping)).thenReturn(filteredTables);
    List<String> result = handler.get_all_tables(DB_P);
    assertThat(result, is(filteredTables));
  }

  @Test
  public void get_table() throws TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    Table table = new Table();
    Table outbound = new Table();
    when(primaryClient.get_table("inbound", "table")).thenReturn(table);
    when(primaryMapping.transformOutboundTable(table)).thenReturn(outbound);
    Table result = handler.get_table(DB_P, "table");
    assertThat(result, is(outbound));
  }

  @Test
  public void get_table_objects_by_name() throws TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    Table table = new Table();
    Table outbound = new Table();
    List<String> tables = Lists.newArrayList("table");
    when(primaryClient.get_table_objects_by_name("inbound", tables)).thenReturn(Lists.newArrayList(table));
    when(primaryMapping.transformOutboundTable(table)).thenReturn(outbound);
    when(databaseMappingService.filterTables(DB_P, tables, primaryMapping)).thenReturn(tables);
    List<Table> result = handler.get_table_objects_by_name(DB_P, tables);
    List<Table> expected = Lists.newArrayList(outbound);
    assertThat(result, is(expected));
  }

  @Test
  public void get_table_names_by_filter() throws TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    List<String> tables = Lists.newArrayList("table1");
    when(primaryClient.get_table_names_by_filter("inbound", "*", (short) 2)).thenReturn(tables);
    when(databaseMappingService.filterTables(DB_P, tables, primaryMapping)).thenReturn(tables);
    List<String> result = handler.get_table_names_by_filter(DB_P, "*", (short) 2);
    assertThat(result, is(tables));
  }

  @Test
  public void alter_table() throws TException {
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
  public void alter_table_with_environment_context() throws TException {
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
  public void add_partition() throws TException {
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
  public void add_partition_with_environment_context() throws TException {
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
  public void add_partitions() throws TException {
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
  public void add_partitions_pspec() throws TException {
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
  public void append_partition() throws TException {
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
  public void add_partitions_req() throws TException {
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
  public void append_partition_with_environment_context() throws TException {
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
  public void append_partition_by_name() throws TException {
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
  public void append_partition_by_name_with_environment_context() throws TException {
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
  public void drop_partition() throws TException {
    List<String> partVals = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.drop_partition("inbound", "table1", partVals, false)).thenReturn(true);
    boolean result = handler.drop_partition(DB_P, "table1", partVals, false);
    assertThat(result, is(true));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void drop_partition_with_environment_context() throws TException {
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
  public void drop_partition_by_name() throws TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.drop_partition_by_name("inbound", "table1", "partName", false)).thenReturn(true);
    boolean result = handler.drop_partition_by_name(DB_P, "table1", "partName", false);
    assertThat(result, is(true));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void drop_partition_by_name_with_environment_context() throws TException {
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
  public void drop_partitions_req() throws TException {
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
  public void get_partition() throws TException {
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
  public void exchange_partition() throws TException {
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
  public void get_partition_with_auth() throws TException {
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
  public void get_partition_by_name() throws TException {
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
  public void get_partitions() throws TException {
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
  public void get_partitions_with_auth() throws TException {
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
  public void get_partitions_pspec() throws TException {
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
  public void get_partition_names() throws TException {
    List<String> partitions = Lists.newArrayList();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryClient.get_partition_names("inbound", "table", (short) 10)).thenReturn(partitions);
    List<String> result = handler.get_partition_names(DB_P, "table", (short) 10);
    assertThat(result, is(partitions));
    verify(primaryMapping, never()).checkWritePermissions(DB_P);
  }

  @Test
  public void get_partitions_ps() throws TException {
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
  public void get_partitions_ps_with_auth() throws TException {
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
  public void get_partition_names_ps() throws TException {
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
  public void get_partitions_by_filter() throws TException {
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
  public void get_part_specs_by_filter() throws TException {
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
  public void get_partitions_by_expr() throws TException {
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
  public void get_partitions_by_names() throws TException {
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
    when(waggleDanceConfiguration.isQueryFunctionsAcrossAllMetastores()).thenReturn(true);
    PanopticOperationHandler panopticHandler = Mockito.mock(PanopticOperationHandler.class);
    when(databaseMappingService.getPanopticOperationHandler()).thenReturn(panopticHandler);
    DatabaseMapping mapping = Mockito.mock(DatabaseMapping.class);
    List<DatabaseMapping> mappings = Lists.newArrayList(mapping);
    when(databaseMappingService.getAvailableDatabaseMappings()).thenReturn(mappings);
    GetAllFunctionsResponse getAllFunctionsResponse = Mockito.mock(GetAllFunctionsResponse.class);
    when(panopticHandler.getAllFunctions(mappings)).thenReturn(getAllFunctionsResponse);
    GetAllFunctionsResponse result = handler.get_all_functions();
    assertThat(result, is(getAllFunctionsResponse));
  }

  @Test
  public void get_all_functionsViaPrimary() throws TException {
    when(waggleDanceConfiguration.isQueryFunctionsAcrossAllMetastores()).thenReturn(false);
    GetAllFunctionsResponse getAllFunctionsResponse = Mockito.mock(GetAllFunctionsResponse.class);
    when(primaryClient.get_all_functions()).thenReturn(getAllFunctionsResponse);

    GetAllFunctionsResponse result = handler.get_all_functions();
    assertThat(result, is(getAllFunctionsResponse));
  }

  @Test
  public void set_ugi() throws TException {
    PanopticOperationHandler panopticHandler = Mockito.mock(PanopticOperationHandler.class);
    when(databaseMappingService.getPanopticOperationHandler()).thenReturn(panopticHandler);
    when(databaseMappingService.getAllDatabaseMappings()).thenReturn(Collections.singletonList(primaryMapping));
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
  public void get_tables_by_type() throws TException {
    List<String> tables = Arrays.asList("tbl0", "tbl1");
    when(primaryClient.get_tables_by_type(DB_P, "tbl*", "EXTERNAL_TABLE")).thenReturn(tables);
    when(databaseMappingService.filterTables(DB_P, tables, primaryMapping)).thenReturn(tables);
    List<String> tablesResult = handler.get_tables_by_type(DB_P, "tbl*", TableType.EXTERNAL_TABLE.name());
    verify(primaryClient).get_tables_by_type(DB_P, "tbl*", "EXTERNAL_TABLE");
    assertThat(tablesResult.size(), is(2));
    assertThat(tablesResult.get(0), is("tbl0"));
    assertThat(tablesResult.get(1), is("tbl1"));
  }

  @Test
  public void get_table_req() throws TException {
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
  public void get_table_objects_by_name_req() throws TException {
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
  public void get_foreign_keys() throws TException {
    ForeignKeysRequest request = new ForeignKeysRequest();
    request.setParent_db_name(null);
    request.setParent_tbl_name(null);
    request.setForeign_db_name(DB_S);
    request.setForeign_tbl_name("table");
    SQLForeignKey key = new SQLForeignKey();
    key.setFktable_db(DB_S);
    key.setFktable_name("table");
    ForeignKeysResponse response = new ForeignKeysResponse(Collections.singletonList(key));

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
  public void getPrivilegeSet() throws TException {
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
  public void getPrivilegeSetDbNameIsNullShouldUsePrimary() throws TException {
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

  @Test
  public void alter_partition() throws TException {
    Partition newPartition = new Partition();
    newPartition.setDbName(DB_P);
    Partition inbound = new Partition();
    when(primaryMapping.transformInboundPartition(newPartition)).thenReturn(inbound);
    handler.alter_partition(DB_P, "table", newPartition);
    verify(primaryMapping, times(2)).checkWritePermissions(DB_P);
    verify(primaryClient).alter_partition(DB_P, "table", inbound);
  }

  @Test
  public void alter_partitions() throws TException {
    Partition newPartition1 = new Partition();
    newPartition1.setDbName(DB_P);
    Partition newPartition2 = new Partition();
    newPartition2.setDbName(DB_P);
    List<Partition> inbound = Lists.newArrayList(new Partition());
    List<Partition> partitions = Lists.newArrayList(newPartition1, newPartition2);
    when(primaryMapping.transformInboundPartitions(partitions)).thenReturn(inbound);
    handler.alter_partitions(DB_P, "table", partitions);
    verify(primaryMapping, times(3)).checkWritePermissions(DB_P);
    verify(primaryClient).alter_partitions(DB_P, "table", inbound);
  }

  @Test
  public void alter_partition_with_environment_context() throws TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    Partition newPartition = new Partition();
    newPartition.setDbName(DB_P);
    Partition inbound = new Partition();
    when(primaryMapping.transformInboundPartition(newPartition)).thenReturn(inbound);
    handler.alter_partition_with_environment_context(DB_P, "table", newPartition, environmentContext);
    verify(primaryMapping, times(2)).checkWritePermissions(DB_P);
    verify(primaryClient).alter_partition_with_environment_context(DB_P, "table", inbound, environmentContext);
  }

  @Test
  public void rename_partition() throws TException {
    Partition newPartition = new Partition();
    newPartition.setDbName(DB_P);
    handler.rename_partition(DB_P, "table", Collections.emptyList(), newPartition);
    verify(primaryMapping, times(2)).checkWritePermissions(DB_P);
    verify(primaryClient).rename_partition(DB_P, "table", Collections.emptyList(), newPartition);
  }

  @Test
  public void partition_name_has_valid_characters() throws TException {
    List<String> partitionValues = Collections.singletonList("name");
    when(primaryClient.partition_name_has_valid_characters(partitionValues, true)).thenReturn(true);
    boolean result = handler.partition_name_has_valid_characters(partitionValues, true);
    assertThat(result, is(true));
  }

  @Test
  public void get_config_value() throws TException {
    String expected = "anotherValue";
    when(primaryClient.get_config_value("name", "defaultValue")).thenReturn(expected);
    String result = handler.get_config_value("name", "defaultValue");
    assertThat(result, is(expected));
  }

  @Test
  public void partition_name_to_vals() throws TException {
    List<String> expected = Arrays.asList("name1", "name2");
    when(primaryClient.partition_name_to_vals("name")).thenReturn(expected);
    List<String> result = handler.partition_name_to_vals("name");
    assertThat(result, is(expected));
  }

  @Test
  public void partition_name_to_spec() throws TException {
    Map<String, String> expected = new HashMap<>();
    when(primaryClient.partition_name_to_spec("name")).thenReturn(expected);
    Map<String, String> result = handler.partition_name_to_spec("name");
    assertThat(result, is(expected));
  }

  @Test
  public void markPartitionForEvent() throws TException {
    Map<String, String> partitionValues = new HashMap<>();
    PartitionEventType partitionEventType = PartitionEventType.findByValue(1);
    handler.markPartitionForEvent(DB_P, "table", partitionValues, partitionEventType);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).markPartitionForEvent(DB_P, "table", partitionValues, partitionEventType);
  }

  @Test
  public void isPartitionMarkedForEvent() throws TException {
    Map<String, String> partitionValues = new HashMap<>();
    PartitionEventType partitionEventType = PartitionEventType.findByValue(1);
    when(primaryClient.isPartitionMarkedForEvent(DB_P, "table", partitionValues, partitionEventType)).thenReturn(true);
    boolean result = handler.isPartitionMarkedForEvent(DB_P, "table", partitionValues, partitionEventType);
    assertThat(result, is(true));
  }

  @Test
  public void create_ischema() throws TException {
    ISchema newISchema = new ISchema();
    newISchema.setDbName(DB_P);
    ISchema inboundISchema = new ISchema();
    inboundISchema.setDbName(DB_P);

    when(primaryMapping.transformInboundISchema(newISchema)).thenReturn(inboundISchema);
    handler.create_ischema(newISchema);

    verify(primaryClient).create_ischema(inboundISchema);
    verify(primaryMapping, times(2)).checkWritePermissions(DB_P);
  }

  @Test
  public void alter_ischema() throws TException {
    AlterISchemaRequest alterISchemaRequest = new AlterISchemaRequest();
    ISchemaName oldISchema = new ISchemaName();
    oldISchema.setDbName(DB_P);
    oldISchema.setCatName(CAT_1);
    oldISchema.setSchemaName("oldSchema");

    ISchema newISchema = new ISchema();
    newISchema.setDbName(DB_P);
    newISchema.setCatName(CAT_1);
    newISchema.setName("newSchema");

    alterISchemaRequest.setName(oldISchema);
    alterISchemaRequest.setNewSchema(newISchema);

    when(primaryMapping.transformInboundAlterISchemaRequest(alterISchemaRequest)).thenReturn(new AlterISchemaRequest());

    handler.alter_ischema(alterISchemaRequest);
    verify(primaryMapping, times(2)).checkWritePermissions(DB_P);
    verify(primaryClient).alter_ischema(new AlterISchemaRequest());
  }

  @Test
  public void drop_ischema() throws TException {
    ISchemaName iSchemaName = new ISchemaName();
    iSchemaName.setDbName(DB_P);
    iSchemaName.setCatName(CAT_1);
    iSchemaName.setSchemaName(SCH_1);

    ISchemaName inboundISchemaName = new ISchemaName();
    inboundISchemaName.setDbName(DB_P);
    inboundISchemaName.setCatName(CAT_1);
    inboundISchemaName.setSchemaName(SCH_1);

    when(primaryMapping.transformInboundISchemaName(iSchemaName)).thenReturn(inboundISchemaName);

    handler.drop_ischema(iSchemaName);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).drop_ischema(iSchemaName);
  }

  @Test
  public void get_ischema() throws TException {
    ISchemaName iSchemaName = new ISchemaName();
    iSchemaName.setDbName(DB_P);
    iSchemaName.setCatName(CAT_1);
    iSchemaName.setSchemaName(SCH_1);

    ISchemaName inboundISchemaName = new ISchemaName();
    inboundISchemaName.setDbName(DB_P);
    inboundISchemaName.setCatName(CAT_1);
    inboundISchemaName.setSchemaName(SCH_1);

    ISchema outboundISchema = new ISchema();
    outboundISchema.setDbName(DB_P);
    outboundISchema.setCatName(CAT_1);
    outboundISchema.setName(SCH_1);

    when(primaryMapping.transformInboundISchemaName(iSchemaName)).thenReturn(inboundISchemaName);
    when(primaryClient.get_ischema(inboundISchemaName)).thenReturn(new ISchema());
    when(primaryMapping.transformOutboundISchema(new ISchema())).thenReturn(outboundISchema);
    ISchema result = handler.get_ischema(iSchemaName);
    assertThat(result, is(outboundISchema));
  }

  @Test
  public void update_table_column_statistics() throws TException {
    ColumnStatisticsDesc columnStatisticsDesc = new ColumnStatisticsDesc(true, DB_P, "table");
    ColumnStatistics columnStatistics = new ColumnStatistics(columnStatisticsDesc, Collections.emptyList());
    ColumnStatistics inboundColumnStatistics = new ColumnStatistics();
    when(primaryMapping.transformInboundColumnStatistics(columnStatistics)).thenReturn(inboundColumnStatistics);
    when(primaryClient.update_table_column_statistics(inboundColumnStatistics)).thenReturn(true);
    boolean result = handler.update_table_column_statistics(columnStatistics);
    verify(primaryMapping).checkWritePermissions(DB_P);
    assertThat(result, is(true));
  }

  @Test
  public void update_partition_column_statistics() throws TException {
    ColumnStatisticsDesc columnStatisticsDesc = new ColumnStatisticsDesc(true, DB_P, "table");
    ColumnStatistics columnStatistics = new ColumnStatistics(columnStatisticsDesc, Collections.emptyList());
    ColumnStatistics inboundColumnStatistics = new ColumnStatistics();
    when(primaryMapping.transformInboundColumnStatistics(columnStatistics)).thenReturn(inboundColumnStatistics);
    when(primaryClient.update_partition_column_statistics(inboundColumnStatistics)).thenReturn(true);
    boolean result = handler.update_partition_column_statistics(columnStatistics);
    verify(primaryMapping).checkWritePermissions(DB_P);
    assertThat(result, is(true));
  }

  @Test
  public void get_table_column_statistics() throws TException {
    ColumnStatistics columnStatistics = new ColumnStatistics();
    ColumnStatistics outboundColumnStatistics = new ColumnStatistics();
    when(primaryClient.get_table_column_statistics(DB_P, "table", "columnName")).thenReturn(columnStatistics);
    when(primaryMapping.transformOutboundColumnStatistics(columnStatistics)).thenReturn(outboundColumnStatistics);
    ColumnStatistics result = handler.get_table_column_statistics(DB_P, "table", "columnName");
    assertThat(result, is(outboundColumnStatistics));
  }

  @Test
  public void get_partition_column_statistics() throws TException {
    ColumnStatistics columnStatistics = new ColumnStatistics();
    ColumnStatistics outboundColumnStatistics = new ColumnStatistics();
    when(primaryClient.get_partition_column_statistics(DB_P, "table", "partitionName", "columnName"))
        .thenReturn(columnStatistics);
    when(primaryMapping.transformOutboundColumnStatistics(columnStatistics)).thenReturn(outboundColumnStatistics);
    ColumnStatistics result = handler.get_partition_column_statistics(DB_P, "table", "partitionName", "columnName");
    assertThat(result, is(outboundColumnStatistics));
  }

  @Test
  public void get_table_statistics_req() throws TException {
    TableStatsRequest tableStatsRequest = new TableStatsRequest(DB_P, "table", Collections.emptyList());
    TableStatsRequest inboundTableStatsRequest = new TableStatsRequest();
    TableStatsResult expected = new TableStatsResult();
    when(primaryMapping.transformInboundTableStatsRequest(tableStatsRequest)).thenReturn(inboundTableStatsRequest);
    when(primaryClient.get_table_statistics_req(inboundTableStatsRequest)).thenReturn(expected);
    TableStatsResult result = handler.get_table_statistics_req(tableStatsRequest);
    assertThat(result, is(expected));
  }

  @Test
  public void get_partitions_statistics_req() throws TException {
    PartitionsStatsRequest request = new PartitionsStatsRequest(DB_P, "table", Collections.emptyList(),
        Collections.emptyList());
    PartitionsStatsRequest inboundRequest = new PartitionsStatsRequest();
    PartitionsStatsResult expected = new PartitionsStatsResult();
    when(primaryMapping.transformInboundPartitionsStatsRequest(request)).thenReturn(inboundRequest);
    when(primaryClient.get_partitions_statistics_req(inboundRequest)).thenReturn(expected);
    PartitionsStatsResult result = handler.get_partitions_statistics_req(request);
    assertThat(result, is(expected));
  }

  @Test
  public void get_aggr_stats_for() throws TException {
    PartitionsStatsRequest request = new PartitionsStatsRequest(DB_P, "table", Collections.emptyList(),
        Collections.emptyList());
    PartitionsStatsRequest inboundRequest = new PartitionsStatsRequest();
    AggrStats expected = new AggrStats();
    when(primaryMapping.transformInboundPartitionsStatsRequest(request)).thenReturn(inboundRequest);
    when(primaryClient.get_aggr_stats_for(inboundRequest)).thenReturn(expected);
    AggrStats result = handler.get_aggr_stats_for(request);
    assertThat(result, is(expected));
  }

  @Test
  public void set_aggr_stats_for() throws TException {
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, DB_P, "table");
    ColumnStatistics colStatistics = new ColumnStatistics(statsDesc, Collections.emptyList());
    List<ColumnStatistics> colStats = Collections.singletonList(colStatistics);
    SetPartitionsStatsRequest request = new SetPartitionsStatsRequest(colStats);
    SetPartitionsStatsRequest inboundRequest = new SetPartitionsStatsRequest();
    when(primaryMapping.transformInboundSetPartitionStatsRequest(request)).thenReturn(inboundRequest);
    when(primaryClient.set_aggr_stats_for(inboundRequest)).thenReturn(true);
    boolean result = handler.set_aggr_stats_for(request);
    assertThat(result, is(true));
  }

  @Test
  public void delete_partition_column_statistics() throws TException {
    when(primaryClient.delete_partition_column_statistics(DB_P, "table", "partition", "column")).thenReturn(true);
    boolean result = handler.delete_partition_column_statistics(DB_P, "table", "partition", "column");
    assertThat(result, is(true));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void delete_table_column_statistics() throws TException {
    when(primaryClient.delete_table_column_statistics(DB_P, "table", "column")).thenReturn(true);
    boolean result = handler.delete_table_column_statistics(DB_P, "table", "column");
    assertThat(result, is(true));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void create_function() throws TException {
    Function function = new Function();
    function.setDbName(DB_P);
    Function inboundFunction = new Function();
    when(primaryMapping.transformInboundFunction(function)).thenReturn(inboundFunction);
    handler.create_function(function);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).create_function(inboundFunction);
  }

  @Test
  public void drop_function() throws TException {
    handler.drop_function(DB_P, "function");
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).drop_function(DB_P, "function");
  }

  @Test
  public void alter_function() throws TException {
    Function newFunc = new Function();
    newFunc.setDbName(DB_P);
    Function inboundFunction = new Function();
    when(primaryMapping.transformInboundFunction(newFunc)).thenReturn(inboundFunction);
    handler.alter_function(DB_P, "function", newFunc);
    verify(primaryMapping, times(2)).checkWritePermissions(DB_P);
    verify(primaryClient).alter_function(DB_P, "function", inboundFunction);
  }

  @Test
  public void get_functions() throws TException {
    List<String> expected = Arrays.asList("func1", "func2");
    when(primaryClient.get_functions(DB_P, "pattern")).thenReturn(expected);
    List<String> result = handler.get_functions(DB_P, "pattern");
    assertThat(result, is(expected));
  }

  @Test
  public void get_function() throws TException {
    Function fromClient = new Function();
    Function expected = new Function();
    when(primaryClient.get_function(DB_P, "funcName")).thenReturn(fromClient);
    when(primaryMapping.transformOutboundFunction(fromClient)).thenReturn(expected);
    Function result = handler.get_function(DB_P, "funcName");
    assertThat(result, is(expected));
  }

  @Test
  public void create_role() throws TException {
    Role role = new Role();
    handler.create_role(role);
    verify(primaryClient).create_role(role);
  }

  @Test
  public void drop_role() throws TException {
    handler.drop_role("role");
    verify(primaryClient).drop_role("role");
  }

  @Test
  public void get_role_names() throws TException {
    List<String> roleNames = Arrays.asList("role1", "role2");
    when(primaryClient.get_role_names()).thenReturn(roleNames);
    assertThat(handler.get_role_names(), is(roleNames));
  }

  @Test
  public void grant_role() throws TException {
    PrincipalType principalType = PrincipalType.findByValue(3);
    handler.grant_role("role", "principal", principalType, "grantor", principalType, true);
    verify(primaryClient).grant_role("role", "principal", principalType, "grantor", principalType, true);
  }

  @Test
  public void revoke_role() throws TException {
    PrincipalType principalType = PrincipalType.findByValue(3);
    handler.revoke_role("role", "principal", principalType);
    verify(primaryClient).revoke_role("role", "principal", principalType);
  }

  @Test
  public void list_roles() throws TException {
    PrincipalType principalType = PrincipalType.findByValue(3);
    handler.list_roles("role", principalType);
    verify(primaryClient).list_roles("role", principalType);
  }

  @Test
  public void grant_revoke_role() throws TException {
    GrantRevokeRoleRequest request = new GrantRevokeRoleRequest();
    handler.grant_revoke_role(request);
    verify(primaryClient).grant_revoke_role(request);
  }

  @Test
  public void get_principals_in_role() throws TException {
    GetPrincipalsInRoleRequest request = new GetPrincipalsInRoleRequest();
    handler.get_principals_in_role(request);
    verify(primaryClient).get_principals_in_role(request);
  }

  @Test
  public void get_role_grants_for_principal() throws TException {
    GetRoleGrantsForPrincipalRequest request = new GetRoleGrantsForPrincipalRequest();
    handler.get_role_grants_for_principal(request);
    verify(primaryClient).get_role_grants_for_principal(request);
  }

  @Test
  public void list_privileges() throws TException {
    PrincipalType principalType = PrincipalType.findByValue(3);
    HiveObjectRef hiveObjectRef = new HiveObjectRef();
    hiveObjectRef.setDbName(DB_P);
    HiveObjectRef inboundHiveObjectRef = new HiveObjectRef();
    when(primaryMapping.transformInboundHiveObjectRef(hiveObjectRef)).thenReturn(inboundHiveObjectRef);
    handler.list_privileges("name", principalType, hiveObjectRef);
    verify(primaryClient).list_privileges("name", principalType, inboundHiveObjectRef);
  }

  @Test
  public void grant_privileges() throws TException {
    HiveObjectRef hiveObjectRef = new HiveObjectRef();
    hiveObjectRef.setDbName(DB_P);
    HiveObjectPrivilege hiveObjectPrivilege = new HiveObjectPrivilege();
    hiveObjectPrivilege.setHiveObject(hiveObjectRef);
    PrivilegeBag privileges = new PrivilegeBag(Collections.singletonList((hiveObjectPrivilege)));
    PrivilegeBag inboundPrivileges = new PrivilegeBag();
    when(primaryMapping.transformInboundPrivilegeBag(privileges)).thenReturn(inboundPrivileges);
    handler.grant_privileges(privileges);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).grant_privileges(inboundPrivileges);
  }

  @Test
  public void revoke_privileges() throws TException {
    HiveObjectRef hiveObjectRef = new HiveObjectRef();
    hiveObjectRef.setDbName(DB_P);
    HiveObjectPrivilege hiveObjectPrivilege = new HiveObjectPrivilege();
    hiveObjectPrivilege.setHiveObject(hiveObjectRef);
    PrivilegeBag privileges = new PrivilegeBag(Collections.singletonList((hiveObjectPrivilege)));
    PrivilegeBag inboundPrivileges = new PrivilegeBag();
    when(primaryMapping.transformInboundPrivilegeBag(privileges)).thenReturn(inboundPrivileges);
    handler.revoke_privileges(privileges);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).revoke_privileges(inboundPrivileges);
  }

  @Test
  public void grant_revoke_privileges() throws TException {
    HiveObjectRef hiveObjectRef = new HiveObjectRef();
    hiveObjectRef.setDbName(DB_P);
    HiveObjectPrivilege hiveObjectPrivilege = new HiveObjectPrivilege();
    hiveObjectPrivilege.setHiveObject(hiveObjectRef);
    PrivilegeBag privileges = new PrivilegeBag(Collections.singletonList((hiveObjectPrivilege)));

    GrantRevokeType grantRevokeType = GrantRevokeType.GRANT;

    GrantRevokePrivilegeRequest request = new GrantRevokePrivilegeRequest(grantRevokeType, privileges);
    GrantRevokePrivilegeRequest inboundRequest = new GrantRevokePrivilegeRequest();
    GrantRevokePrivilegeResponse expected = new GrantRevokePrivilegeResponse();
    when(primaryMapping.transformInboundGrantRevokePrivilegesRequest(request)).thenReturn(inboundRequest);
    when(primaryClient.grant_revoke_privileges(inboundRequest)).thenReturn(expected);
    GrantRevokePrivilegeResponse response = handler.grant_revoke_privileges(request);
    assertThat(response, is(expected));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void get_delegation_token() throws TException, IOException, InterruptedException {
    String expected = "expected";
    when(saslServerWrapper.getDelegationTokenManager()).thenReturn(metastoreDelegationTokenManager);
    when(metastoreDelegationTokenManager.getDelegationToken("owner", "kerberos_principal",
        null)).thenReturn(expected);
    String result = handler.get_delegation_token("owner", "kerberos_principal");
    assertThat(result, is(expected));
  }

  @Test
  public void renew_delegation_token() throws TException, IOException {
    long expected = 10L;
    when(saslServerWrapper.getDelegationTokenManager()).thenReturn(metastoreDelegationTokenManager);
    when(metastoreDelegationTokenManager.renewDelegationToken("token")).thenReturn(expected);
    long result = handler.renew_delegation_token("token");
    assertThat(result, is(expected));
  }

  @Test
  public void cancel_delegation_token() throws TException, IOException {
    when(saslServerWrapper.getDelegationTokenManager()).thenReturn(metastoreDelegationTokenManager);
    handler.cancel_delegation_token("token");
    verify(metastoreDelegationTokenManager).cancelDelegationToken("token");
  }

  @Test
  public void get_open_txns() throws TException {
    GetOpenTxnsResponse expected = new GetOpenTxnsResponse();
    when(primaryClient.get_open_txns()).thenReturn(expected);
    GetOpenTxnsResponse result = handler.get_open_txns();
    assertThat(result, is(expected));
  }

  @Test
  public void get_open_txns_info() throws TException {
    GetOpenTxnsInfoResponse expected = new GetOpenTxnsInfoResponse();
    when(primaryClient.get_open_txns_info()).thenReturn(expected);
    GetOpenTxnsInfoResponse result = handler.get_open_txns_info();
    assertThat(result, is(expected));
  }

  @Test
  public void open_txns() throws TException {
    OpenTxnRequest request = new OpenTxnRequest();
    OpenTxnsResponse expected = new OpenTxnsResponse();
    when(primaryClient.open_txns(request)).thenReturn(expected);
    OpenTxnsResponse result = handler.open_txns(request);
    assertThat(result, is(expected));
  }

  @Test
  public void abort_txn() throws TException {
    AbortTxnRequest request = new AbortTxnRequest();
    handler.abort_txn(request);
    verify(primaryClient).abort_txn(request);
  }

  @Test
  public void commit_txn() throws TException {
    CommitTxnRequest request = new CommitTxnRequest();
    handler.commit_txn(request);
    verify(primaryClient).commit_txn(request);
  }

  @Test
  public void lock() throws TException {
    LockComponent lockComponent = new LockComponent(LockType.EXCLUSIVE, LockLevel.DB, DB_P);
    LockRequest lockRequest = new LockRequest(Collections.singletonList(lockComponent), "user", "host");
    LockRequest inboundRequest = new LockRequest();
    LockResponse expected = new LockResponse();
    when(primaryMapping.transformInboundLockRequest(lockRequest)).thenReturn(inboundRequest);
    when(primaryClient.lock(inboundRequest)).thenReturn(expected);
    LockResponse result = handler.lock(lockRequest);
    assertThat(result, is(expected));
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void check_lock() throws TException {
    CheckLockRequest request = new CheckLockRequest();
    LockResponse expected = new LockResponse();
    when(primaryClient.check_lock(request)).thenReturn(expected);
    LockResponse result = handler.check_lock(request);
    assertThat(result, is(expected));
  }

  @Test
  public void unlock() throws TException {
    UnlockRequest request = new UnlockRequest();
    handler.unlock(request);
    verify(primaryClient).unlock(request);
  }

  @Test
  public void show_locks() throws TException {
    ShowLocksRequest request = new ShowLocksRequest();
    ShowLocksResponse expected = new ShowLocksResponse();
    when(primaryClient.show_locks(request)).thenReturn(expected);
    ShowLocksResponse result = handler.show_locks(request);
    assertThat(result, is(expected));
  }

  @Test
  public void heartbeat() throws TException {
    HeartbeatRequest request = new HeartbeatRequest();
    handler.heartbeat(request);
    verify(primaryClient).heartbeat(request);
  }

  @Test
  public void heartbeat_txn_range() throws TException {
    HeartbeatTxnRangeRequest request = new HeartbeatTxnRangeRequest();
    HeartbeatTxnRangeResponse expected = new HeartbeatTxnRangeResponse();
    when(primaryClient.heartbeat_txn_range(request)).thenReturn(expected);
    HeartbeatTxnRangeResponse result = handler.heartbeat_txn_range(request);
    assertThat(result, is(expected));
  }

  @Test
  public void compact() throws TException {
    CompactionRequest request = new CompactionRequest(DB_P, "table", CompactionType.MAJOR);
    CompactionRequest inboundRequest = new CompactionRequest();
    when(primaryMapping.transformInboundCompactionRequest(request)).thenReturn(inboundRequest);
    handler.compact(request);
    verify(primaryClient).compact(inboundRequest);
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void show_compact() throws TException {
    ShowCompactRequest request = new ShowCompactRequest();
    ShowCompactResponse expected = new ShowCompactResponse();
    when(primaryClient.show_compact(request)).thenReturn(expected);
    ShowCompactResponse result = handler.show_compact(request);
    assertThat(result, is(expected));
  }

  @Test
  public void getCpuProfile() throws TException {
    String expected = "expected";
    when(primaryClient.getCpuProfile(10)).thenReturn(expected);
    String result = handler.getCpuProfile(10);
    assertThat(result, is(expected));
  }

  @Test
  public void getVersion() throws TException {
    String expected = "version";
    when(primaryClient.getVersion()).thenReturn(expected);
    String result = handler.getVersion();
    assertThat(result, is(expected));
  }

  @Test
  public void getStatus() throws TException {
    fb_status expected = fb_status.ALIVE;
    when(primaryClient.getStatus()).thenReturn(expected);
    fb_status result = handler.getStatus();
    assertThat(result, is(expected));
  }

  @Test
  public void getConf() {
    Configuration expected = new Configuration();
    handler.setConf(expected);
    Configuration result = handler.getConf();
    assertThat(result, is(expected));
  }

  @Test
  public void abort_txns() throws TException {
    AbortTxnsRequest request = new AbortTxnsRequest();
    handler.abort_txns(request);
    verify(primaryClient).abort_txns(request);
  }

  @Test
  public void add_dynamic_partitions() throws TException {
    AddDynamicPartitions request = new AddDynamicPartitions(1, 1, DB_P, "table", Collections.emptyList());
    AddDynamicPartitions inboundRequest = new AddDynamicPartitions();
    when(primaryMapping.transformInboundAddDynamicPartitions(request)).thenReturn(inboundRequest);
    handler.add_dynamic_partitions(request);
    verify(primaryClient).add_dynamic_partitions(inboundRequest);
    verify(primaryMapping).checkWritePermissions(DB_P);
  }

  @Test
  public void add_foreign_key() throws TException {
    AddForeignKeyRequest request = new AddForeignKeyRequest();
    when(primaryMapping.transformInboundAddForeignKeyRequest(request)).thenReturn(request);
    handler.add_foreign_key(request);
    verify(primaryClient).add_foreign_key(request);
  }

  @Test
  public void add_master_key() throws TException {
    int expected = 10;
    when(primaryClient.add_master_key("key")).thenReturn(expected);
    int result = handler.add_master_key("key");
    assertThat(result, is(expected));
  }

  @Test
  public void add_primary_key() throws TException {
    AddPrimaryKeyRequest request = new AddPrimaryKeyRequest();
    handler.add_primary_key(request);
    verify(primaryClient).add_primary_key(request);
  }

  @Test
  public void add_token() throws TException {
    when(primaryClient.add_token("identifier", "delegation")).thenReturn(true);
    boolean result = handler.add_token("identifier", "delegation");
    assertThat(result, is(true));
  }

  @Test
  public void alter_partitions_with_environment_context() throws TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    Partition newPartition1 = new Partition();
    newPartition1.setDbName(DB_P);
    Partition newPartition2 = new Partition();
    newPartition2.setDbName(DB_P);
    List<Partition> inbound = Lists.newArrayList(new Partition(), new Partition());
    List<Partition> partitions = Lists.newArrayList(newPartition1, newPartition2);
    when(primaryMapping.transformInboundPartitions(partitions)).thenReturn(inbound);
    handler.alter_partitions_with_environment_context(DB_P, "table", partitions, environmentContext);
    verify(primaryMapping, times(3)).checkWritePermissions(DB_P);
    verify(primaryClient)
        .alter_partitions_with_environment_context(DB_P, "table", inbound, environmentContext);
  }

  @Test
  public void alter_table_with_cascade() throws TException {
    Table table = new Table();
    table.setDbName(DB_P);
    Table inbound = new Table();
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn("inbound");
    when(primaryMapping.transformInboundTable(table)).thenReturn(inbound);
    handler.alter_table_with_cascade(DB_P, "table", table, true);
    verify(primaryMapping, times(2)).checkWritePermissions(DB_P);
    verify(primaryClient).alter_table_with_cascade("inbound", "table", inbound, true);
  }

  @Test
  public void cache_file_metadata() throws TException {
    CacheFileMetadataRequest request = new CacheFileMetadataRequest(DB_P, "table");
    CacheFileMetadataRequest inboundRequest = new CacheFileMetadataRequest();
    CacheFileMetadataResult expected = new CacheFileMetadataResult();
    when(primaryMapping.transformInboundCacheFileMetadataRequest(request)).thenReturn(inboundRequest);
    when(primaryClient.cache_file_metadata(inboundRequest)).thenReturn(expected);
    CacheFileMetadataResult result = handler.cache_file_metadata(request);
    assertThat(result, is(expected));
  }

  @Test
  public void clear_file_metadata() throws TException {
    ClearFileMetadataRequest request = new ClearFileMetadataRequest();
    ClearFileMetadataResult expected = new ClearFileMetadataResult();
    when(primaryClient.clear_file_metadata(request)).thenReturn(expected);
    ClearFileMetadataResult result = handler.clear_file_metadata(request);
    assertThat(result, is(expected));
  }

  @Test
  public void create_table_with_constraints() throws TException {
    Table table = new Table();
    table.setDbName(DB_P);
    Table inboundTable = new Table();
    inboundTable.setDbName(DB_P);
    List<SQLPrimaryKey> primaryKeys = Collections.emptyList();
    List<SQLForeignKey> foreignKeys = Collections.emptyList();
    List<SQLUniqueConstraint> uniqueConstraints = Collections.emptyList();
    List<SQLNotNullConstraint> notNullConstraints = Collections.emptyList();
    List<SQLDefaultConstraint> defaultConstraints = Collections.emptyList();
    List<SQLCheckConstraint> checkConstraints = Collections.emptyList();
    when(primaryMapping.transformInboundTable(table)).thenReturn(inboundTable);
    handler.create_table_with_constraints(table, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints,
            defaultConstraints, checkConstraints);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).create_table_with_constraints(table, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints,
            defaultConstraints, checkConstraints);
  }

  @Test
  public void drop_constraint() throws TException {
    DropConstraintRequest request = new DropConstraintRequest(DB_P, "table", "constraint");
    DropConstraintRequest inboundRequest = new DropConstraintRequest();
    when(primaryMapping.transformInboundDropConstraintRequest(request)).thenReturn(inboundRequest);
    handler.drop_constraint(request);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient).drop_constraint(inboundRequest);
  }

  @Test
  public void exchange_partitions() throws TException {
    Map<String, String> partitionSpecs = new HashMap<>();
    List<Partition> partitions = Collections.emptyList();
    List<Partition> expected = Collections.emptyList();
    when(primaryMapping.transformInboundDatabaseName("dest_db")).thenReturn("dest_db");
    when(primaryMapping.transformOutboundPartitions(partitions)).thenReturn(expected);
    when(primaryClient.exchange_partitions(partitionSpecs, DB_P, "source", "dest_db", "dest_table"))
        .thenReturn(partitions);
    List<Partition> result = handler.exchange_partitions(partitionSpecs, DB_P, "source", "dest_db", "dest_table");
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryMapping).checkWritePermissions("dest_db");
    assertThat(result, is(expected));
  }

  @Test
  public void fire_listener_event() throws TException {
    FireEventRequest request = new FireEventRequest();
    request.setDbName(DB_P);
    FireEventRequest inboundRequest = new FireEventRequest();
    FireEventResponse expected = new FireEventResponse();
    when(primaryMapping.transformInboundFireEventRequest(request)).thenReturn(inboundRequest);
    when(primaryClient.fire_listener_event(inboundRequest)).thenReturn(expected);
    FireEventResponse result = handler.fire_listener_event(request);
    assertThat(result, is(expected));
  }

  @Test
  public void get_all_token_identifiers() throws TException {
    List<String> expected = Arrays.asList("token1", "token2");
    when(primaryClient.get_all_token_identifiers()).thenReturn(expected);
    List<String> result = handler.get_all_token_identifiers();
    assertThat(result, is(expected));
  }

  @Test
  public void get_current_notificationEventId() throws TException {
    handler.get_current_notificationEventId();
    verify(primaryClient).get_current_notificationEventId();
  }

  @Test
  public void get_fields_with_environment_context() throws TException {
    EnvironmentContext context = new EnvironmentContext();
    List<FieldSchema> expected = Arrays
        .asList(new FieldSchema("name1", "type1", ""), new FieldSchema("name2", "type2", ""));
    when(primaryClient.get_fields_with_environment_context(DB_P, "table", context)).thenReturn(expected);
    List<FieldSchema> result = handler.get_fields_with_environment_context(DB_P, "table", context);
    assertThat(result, is(expected));
  }

  @Test
  public void get_file_metadata() throws TException {
    GetFileMetadataRequest request = new GetFileMetadataRequest();
    GetFileMetadataResult expected = new GetFileMetadataResult();
    when(primaryClient.get_file_metadata(request)).thenReturn(expected);
    GetFileMetadataResult result = handler.get_file_metadata(request);
    assertThat(result, is(expected));
  }

  @Test
  public void get_file_metadata_by_expr() throws TException {
    GetFileMetadataByExprRequest request = new GetFileMetadataByExprRequest();
    GetFileMetadataByExprResult expected = new GetFileMetadataByExprResult();
    when(primaryClient.get_file_metadata_by_expr(request)).thenReturn(expected);
    GetFileMetadataByExprResult result = handler.get_file_metadata_by_expr(request);
    assertThat(result, is(expected));
  }

  @Test
  public void get_master_keys() throws TException {
    List<String> expected = Arrays.asList("key1", "key2");
    when(primaryClient.get_master_keys()).thenReturn(expected);
    List<String> result = handler.get_master_keys();
    assertThat(result, is(expected));
  }

  @Test
  public void get_next_notification() throws TException {
    NotificationEventRequest request = new NotificationEventRequest();
    NotificationEventResponse expected = new NotificationEventResponse();
    when(primaryClient.get_next_notification(request)).thenReturn(expected);
    NotificationEventResponse result = handler.get_next_notification(request);
    assertThat(result, is(expected));
  }

  @Test
  public void get_num_partitions_by_filter() throws TException {
    int expected = 10;
    when(primaryClient.get_num_partitions_by_filter(DB_P, "table", "filter")).thenReturn(expected);
    int result = handler.get_num_partitions_by_filter(DB_P, "table", "filter");
    assertThat(result, is(expected));
  }

  @Test
  public void get_primary_keys() throws TException {
    PrimaryKeysRequest request = new PrimaryKeysRequest(DB_P, "table");
    PrimaryKeysRequest inboundRequest = new PrimaryKeysRequest();
    PrimaryKeysResponse response = new PrimaryKeysResponse();
    PrimaryKeysResponse expected = new PrimaryKeysResponse();
    when(primaryMapping.transformInboundPrimaryKeysRequest(request)).thenReturn(inboundRequest);
    when(primaryMapping.transformOutboundPrimaryKeysResponse(response)).thenReturn(expected);
    when(primaryClient.get_primary_keys(inboundRequest)).thenReturn(response);
    PrimaryKeysResponse result = handler.get_primary_keys(request);
    assertThat(result, is(expected));
  }

  @Test
  public void get_partition_values() throws TException {
    PartitionValuesRequest request = new PartitionValuesRequest(DB_P, "table",
        Collections.singletonList(new FieldSchema()));
    List<PartitionValuesRow> partitionValues = Collections.singletonList(new PartitionValuesRow());
    PartitionValuesResponse response = new PartitionValuesResponse(partitionValues);
    when(primaryClient.get_partition_values(request)).thenReturn(response);
    when(primaryMapping.transformInboundPartitionValuesRequest(request)).thenReturn(request);
    PartitionValuesResponse result = handler.get_partition_values(request);
    assertThat(result.getPartitionValuesSize(), is(1));
    assertThat(result.getPartitionValues(), is(sameInstance(partitionValues)));
  }

  @Test
  public void create_catalog() throws TException {
    CreateCatalogRequest createCatalogRequest = new CreateCatalogRequest();
    createCatalogRequest.setCatalog(new Catalog(CAT_1, ""));

    doNothing().when(primaryClient).create_catalog(isA(CreateCatalogRequest.class));
    handler.create_catalog(createCatalogRequest);
    verify(primaryClient, times(1)).create_catalog(createCatalogRequest);
  }

  @Test
  public void alter_catalog() throws TException {
    AlterCatalogRequest alterCatalogRequest = new AlterCatalogRequest();
    alterCatalogRequest.setName(CAT_1);
    alterCatalogRequest.setNewCat(new Catalog(CAT_2, ""));

    doNothing().when(primaryClient).alter_catalog(isA(AlterCatalogRequest.class));
    handler.alter_catalog(alterCatalogRequest);
    verify(primaryClient, times(1)).alter_catalog(alterCatalogRequest);
  }


  @Test
  public void get_catalog() throws TException {
    GetCatalogRequest getCatalogRequest = new GetCatalogRequest();
    GetCatalogResponse getCatalogResponse = new GetCatalogResponse();

    when(primaryClient.get_catalog(getCatalogRequest)).thenReturn(getCatalogResponse);
    GetCatalogResponse result = handler.get_catalog(getCatalogRequest);
    assertThat(result, is(getCatalogResponse));
    verify(primaryClient, times(1)).get_catalog(getCatalogRequest);
  }

  @Test
  public void get_catalogs() throws TException {
    GetCatalogsResponse getCatalogsResponse = new GetCatalogsResponse();

    when(primaryClient.get_catalogs()).thenReturn(getCatalogsResponse);
    GetCatalogsResponse result = handler.get_catalogs();
    verify(primaryClient, times(1)).get_catalogs();
  }

  @Test
  public void drop_catalog() throws TException {
    DropCatalogRequest dropCatalogRequest = new DropCatalogRequest();
    doNothing().when(primaryClient).drop_catalog(isA(DropCatalogRequest.class));
    handler.drop_catalog(dropCatalogRequest);
    verify(primaryClient, times(1)).drop_catalog(dropCatalogRequest);
  }

  @Test
  public void truncate_table() throws TException {
    List<String>partNames = Lists.newArrayList();
    handler.truncate_table(DB_P, TBL_1, partNames);
    verify(primaryClient, times(1)).truncate_table(DB_P, TBL_1, partNames);
  }

  @Test
  public void refresh_privileges() throws TException {
    GrantRevokePrivilegeRequest grantRevokePrivilegeRequest = new GrantRevokePrivilegeRequest();
    HiveObjectRef hiveObjectRef = new HiveObjectRef();
    hiveObjectRef.setDbName(DB_P);
    PrivilegeBag privileges = new PrivilegeBag();
    List<HiveObjectPrivilege> privilegesList = new ArrayList<>();
    HiveObjectPrivilege hiveObjectPrivilege = new HiveObjectPrivilege();
    HiveObjectRef hor = new HiveObjectRef();
    hor.setDbName(DB_P);
    hor.setObjectType(HiveObjectType.DATABASE);
    hor.setObjectName(DB_P);
    hiveObjectPrivilege.setHiveObject(hor);
    privilegesList.add(hiveObjectPrivilege);
    privileges.setPrivileges(privilegesList);
    grantRevokePrivilegeRequest.setPrivileges(privileges);
    GrantRevokePrivilegeResponse grantRevokePrivilegeResponse = new GrantRevokePrivilegeResponse();

    when(primaryMapping.transformInboundHiveObjectRef(hiveObjectRef)).thenReturn(hiveObjectRef);
    when(primaryMapping.transformInboundGrantRevokePrivilegesRequest(grantRevokePrivilegeRequest)).thenReturn(grantRevokePrivilegeRequest);
    when(primaryClient.refresh_privileges(hiveObjectRef, "dummy", grantRevokePrivilegeRequest)).thenReturn(grantRevokePrivilegeResponse);
    GrantRevokePrivilegeResponse result = handler.refresh_privileges(hiveObjectRef, "dummy", grantRevokePrivilegeRequest);
    assertThat(result, is(grantRevokePrivilegeResponse));
    verify(primaryClient, times(1)).refresh_privileges(hiveObjectRef, "dummy", grantRevokePrivilegeRequest);
  }

  @Test
  public void repl_tbl_writeid_state() throws TException {
    ReplTblWriteIdStateRequest replTblWriteIdStateRequest = new ReplTblWriteIdStateRequest();
    replTblWriteIdStateRequest.setDbName(DB_P);

    when(primaryMapping.transformInboundReplTblWriteIdStateRequest(replTblWriteIdStateRequest)).thenReturn(replTblWriteIdStateRequest);
    doNothing().when(primaryClient).repl_tbl_writeid_state(isA(ReplTblWriteIdStateRequest.class));
    handler.repl_tbl_writeid_state(replTblWriteIdStateRequest);
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient, times(1)).repl_tbl_writeid_state(replTblWriteIdStateRequest);
  }

  @Test
  public void get_valid_write_ids() throws TException {
    GetValidWriteIdsRequest getValidWriteIdsRequest = new GetValidWriteIdsRequest();
    GetValidWriteIdsResponse getValidWriteIdsResponse = new GetValidWriteIdsResponse();

    when(primaryClient.get_valid_write_ids(getValidWriteIdsRequest)).thenReturn(getValidWriteIdsResponse);
    GetValidWriteIdsResponse result = handler.get_valid_write_ids(getValidWriteIdsRequest);
    assertThat(result, is(getValidWriteIdsResponse));
    verify(primaryClient, times(1)).get_valid_write_ids(getValidWriteIdsRequest);
  }

  @Test
  public void allocate_table_write_ids() throws TException {
    AllocateTableWriteIdsRequest allocateTableWriteIdsRequest = new AllocateTableWriteIdsRequest();
    allocateTableWriteIdsRequest.setDbName(DB_P);
    AllocateTableWriteIdsResponse allocateTableWriteIdsResponse = new AllocateTableWriteIdsResponse();

    when(primaryMapping.transformInboundAllocateTableWriteIdsRequest(allocateTableWriteIdsRequest)).thenReturn(allocateTableWriteIdsRequest);
    when(primaryClient.allocate_table_write_ids(allocateTableWriteIdsRequest)).thenReturn(allocateTableWriteIdsResponse);

    AllocateTableWriteIdsResponse result = handler.allocate_table_write_ids(allocateTableWriteIdsRequest);
    assertThat(result, is(allocateTableWriteIdsResponse));
    verify(primaryMapping).checkWritePermissions(DB_P);
    verify(primaryClient, times(1)).allocate_table_write_ids(allocateTableWriteIdsRequest);
  }

  @Test
  public void add_unique_constraint() throws TException {
    AddUniqueConstraintRequest addUniqueConstraintRequest = new AddUniqueConstraintRequest();
    addUniqueConstraintRequest.setUniqueConstraintCols(Lists.newArrayList(new SQLUniqueConstraint()));

    when(primaryMapping.transformInboundAddUniqueConstraintRequest(addUniqueConstraintRequest)).thenReturn(addUniqueConstraintRequest);
    doNothing().when(primaryClient).add_unique_constraint(isA(AddUniqueConstraintRequest.class));
    handler.add_unique_constraint(addUniqueConstraintRequest);
    verify(primaryClient, times(1)).add_unique_constraint(addUniqueConstraintRequest);
  }

  @Test
  public void add_not_null_constraint() throws TException {
    AddNotNullConstraintRequest addNotNullConstraintRequest = new AddNotNullConstraintRequest();
    addNotNullConstraintRequest.setNotNullConstraintCols(Lists.newArrayList(new SQLNotNullConstraint()));

    when(primaryMapping.transformInboundAddNotNullConstraintRequest(addNotNullConstraintRequest)).thenReturn(addNotNullConstraintRequest);
    doNothing().when(primaryClient).add_not_null_constraint(isA(AddNotNullConstraintRequest.class));
    handler.add_not_null_constraint(addNotNullConstraintRequest);
    verify(primaryClient, times(1)).add_not_null_constraint(addNotNullConstraintRequest);
  }

  @Test
  public void add_default_constraint() throws TException {
    AddDefaultConstraintRequest addDefaultConstraintRequest = new AddDefaultConstraintRequest();
    addDefaultConstraintRequest.setDefaultConstraintCols(Lists.newArrayList(new SQLDefaultConstraint()));

    when(primaryMapping.transformInboundAddDefaultConstraintRequest(addDefaultConstraintRequest)).
            thenReturn(addDefaultConstraintRequest);

    doNothing().when(primaryClient).add_default_constraint(isA(AddDefaultConstraintRequest.class));
    handler.add_default_constraint(addDefaultConstraintRequest);
    verify(primaryClient, times(1)).add_default_constraint(addDefaultConstraintRequest);
  }

  @Test
  public void add_check_constraint() throws TException {
    AddCheckConstraintRequest addCheckConstraintRequest = new AddCheckConstraintRequest();
    addCheckConstraintRequest.setCheckConstraintCols(Lists.newArrayList(new SQLCheckConstraint()));

    when(primaryMapping.transformInboundAddCheckConstraintRequest(addCheckConstraintRequest)).thenReturn(addCheckConstraintRequest);
    doNothing().when(primaryClient).add_check_constraint(isA(AddCheckConstraintRequest.class));
    handler.add_check_constraint(addCheckConstraintRequest);
    verify(primaryClient, times(1)).add_check_constraint(addCheckConstraintRequest);
  }

  @Test
  public void get_metastore_db_uuid() throws TException {
    when(primaryClient.get_metastore_db_uuid()).thenReturn("uuid");
    String result = handler.get_metastore_db_uuid();
    assertThat(result, is("uuid"));
    verify(primaryClient, times(1)).get_metastore_db_uuid();
  }

  @Test
  public void create_resource_plan() throws TException {
    WMCreateResourcePlanRequest wmCreateResourcePlanRequest = new WMCreateResourcePlanRequest();
    WMCreateResourcePlanResponse wmCreateResourcePlanResponse = new WMCreateResourcePlanResponse();

    when(primaryClient.create_resource_plan(wmCreateResourcePlanRequest)).thenReturn(wmCreateResourcePlanResponse);
    WMCreateResourcePlanResponse result = handler.create_resource_plan(wmCreateResourcePlanRequest);
    assertThat(result, is(wmCreateResourcePlanResponse));
    verify(primaryClient, times(1)).create_resource_plan(wmCreateResourcePlanRequest);
  }

  @Test
  public void get_resource_plan() throws TException {
    WMGetResourcePlanRequest wmGetResourcePlanRequest = new WMGetResourcePlanRequest();
    WMGetResourcePlanResponse wmGetResourcePlanResponse = new WMGetResourcePlanResponse();

    when(primaryClient.get_resource_plan(wmGetResourcePlanRequest)).thenReturn(wmGetResourcePlanResponse);
    WMGetResourcePlanResponse result = handler.get_resource_plan(wmGetResourcePlanRequest);
    assertThat(result, is(wmGetResourcePlanResponse));
    verify(primaryClient, times(1)).get_resource_plan(wmGetResourcePlanRequest);
  }

  @Test
  public void get_active_resource_plan() throws TException {
    WMGetActiveResourcePlanRequest wmGetActiveResourcePlanRequest = new WMGetActiveResourcePlanRequest();
    WMGetActiveResourcePlanResponse wmGetActiveResourcePlanResponse = new WMGetActiveResourcePlanResponse();

    when(primaryClient.get_active_resource_plan(wmGetActiveResourcePlanRequest)).thenReturn(wmGetActiveResourcePlanResponse);
    WMGetActiveResourcePlanResponse result = handler.get_active_resource_plan(wmGetActiveResourcePlanRequest);
    assertThat(result, is(wmGetActiveResourcePlanResponse));
    verify(primaryClient, times(1)).get_active_resource_plan(wmGetActiveResourcePlanRequest);
  }

  @Test
  public void get_all_resource_plans() throws TException {
    WMGetAllResourcePlanRequest wmGetAllResourcePlanRequest = new WMGetAllResourcePlanRequest();
    WMGetAllResourcePlanResponse wmGetAllResourcePlanResponse = new WMGetAllResourcePlanResponse();

    when(primaryClient.get_all_resource_plans(wmGetAllResourcePlanRequest)).thenReturn(wmGetAllResourcePlanResponse);
    WMGetAllResourcePlanResponse result = handler.get_all_resource_plans(wmGetAllResourcePlanRequest);
    assertThat(result, is(wmGetAllResourcePlanResponse));
    verify(primaryClient, times(1)).get_all_resource_plans(wmGetAllResourcePlanRequest);
  }

  @Test
  public void alter_resource_plan() throws TException {
    WMAlterResourcePlanRequest wmAlterResourcePlanRequest = new WMAlterResourcePlanRequest();
    WMAlterResourcePlanResponse wmAlterResourcePlanResponse = new WMAlterResourcePlanResponse();

    when(primaryClient.alter_resource_plan(wmAlterResourcePlanRequest)).thenReturn(wmAlterResourcePlanResponse);
    WMAlterResourcePlanResponse result = handler.alter_resource_plan(wmAlterResourcePlanRequest);
    assertThat(result, is(wmAlterResourcePlanResponse));
    verify(primaryClient, times(1)).alter_resource_plan(wmAlterResourcePlanRequest);
  }

  @Test
  public void validate_resource_plan() throws TException {
    WMValidateResourcePlanRequest wmValidateResourcePlanRequest = new WMValidateResourcePlanRequest();
    WMValidateResourcePlanResponse wmValidateResourcePlanResponse = new WMValidateResourcePlanResponse();

    when(primaryClient.validate_resource_plan(wmValidateResourcePlanRequest)).thenReturn(wmValidateResourcePlanResponse);
    WMValidateResourcePlanResponse result = handler.validate_resource_plan(wmValidateResourcePlanRequest);
    assertThat(result, is(wmValidateResourcePlanResponse));
    verify(primaryClient, times(1)).validate_resource_plan(wmValidateResourcePlanRequest);
  }

  @Test
  public void drop_resource_plan() throws TException {
    WMDropResourcePlanRequest wmDropResourcePlanRequest = new WMDropResourcePlanRequest();
    WMDropResourcePlanResponse wmDropResourcePlanResponse = new WMDropResourcePlanResponse();

    when(primaryClient.drop_resource_plan(wmDropResourcePlanRequest)).thenReturn(wmDropResourcePlanResponse);
    WMDropResourcePlanResponse result = handler.drop_resource_plan(wmDropResourcePlanRequest);
    assertThat(result, is(wmDropResourcePlanResponse));
    verify(primaryClient, times(1)).drop_resource_plan(wmDropResourcePlanRequest);
  }

  @Test
  public void create_wm_trigger() throws TException {
    WMCreateTriggerRequest wmCreateTriggerRequest = new WMCreateTriggerRequest();
    WMCreateTriggerResponse wmCreateTriggerResponse = new WMCreateTriggerResponse();

    when(primaryClient.create_wm_trigger(wmCreateTriggerRequest)).thenReturn(wmCreateTriggerResponse);
    WMCreateTriggerResponse result = handler.create_wm_trigger(wmCreateTriggerRequest);
    assertThat(result, is(wmCreateTriggerResponse));
    verify(primaryClient, times(1)).create_wm_trigger(wmCreateTriggerRequest);
  }

  @Test
  public void alter_wm_trigger() throws TException {
    WMAlterTriggerRequest wmAlterTriggerRequest = new WMAlterTriggerRequest();
    WMAlterTriggerResponse wmAlterTriggerResponse = new WMAlterTriggerResponse();

    when(primaryClient.alter_wm_trigger(wmAlterTriggerRequest)).thenReturn(wmAlterTriggerResponse);
    WMAlterTriggerResponse result = handler.alter_wm_trigger(wmAlterTriggerRequest);
    assertThat(result, is(wmAlterTriggerResponse));
    verify(primaryClient, times(1)).alter_wm_trigger(wmAlterTriggerRequest);
  }

  @Test
  public void drop_wm_trigger() throws TException {
    WMDropTriggerRequest wmDropTriggerRequest = new WMDropTriggerRequest();
    WMDropTriggerResponse wmDropTriggerResponse = new WMDropTriggerResponse();

    when(primaryClient.drop_wm_trigger(wmDropTriggerRequest)).thenReturn(wmDropTriggerResponse);
    WMDropTriggerResponse result = handler.drop_wm_trigger(wmDropTriggerRequest);
    assertThat(result, is(wmDropTriggerResponse));
    verify(primaryClient, times(1)).drop_wm_trigger(wmDropTriggerRequest);
  }

  @Test
  public void get_triggers_for_resourceplan() throws TException {
    WMGetTriggersForResourePlanRequest wmGetTriggersForResourePlanRequest = new WMGetTriggersForResourePlanRequest();
    WMGetTriggersForResourePlanResponse wmGetTriggersForResourePlanResponse = new WMGetTriggersForResourePlanResponse();

    when(primaryClient.get_triggers_for_resourceplan(wmGetTriggersForResourePlanRequest)).thenReturn(wmGetTriggersForResourePlanResponse);
    WMGetTriggersForResourePlanResponse result = handler.get_triggers_for_resourceplan(wmGetTriggersForResourePlanRequest);
    assertThat(result, is(wmGetTriggersForResourePlanResponse));
    verify(primaryClient, times(1)).get_triggers_for_resourceplan(wmGetTriggersForResourePlanRequest);
  }

  @Test
  public void create_wm_pool() throws TException {
    WMCreatePoolRequest wmCreatePoolRequest = new WMCreatePoolRequest();
    WMCreatePoolResponse wmCreatePoolResponse = new WMCreatePoolResponse();

    when(primaryClient.create_wm_pool(wmCreatePoolRequest)).thenReturn(wmCreatePoolResponse);
    WMCreatePoolResponse result = handler.create_wm_pool(wmCreatePoolRequest);
    assertThat(result, is(wmCreatePoolResponse));
    verify(primaryClient, times(1)).create_wm_pool(wmCreatePoolRequest);
  }

  @Test
  public void alter_wm_pool() throws TException {
    WMAlterPoolRequest wmAlterPoolRequest = new WMAlterPoolRequest();
    WMAlterPoolResponse wmAlterPoolResponse = new WMAlterPoolResponse();

    when(primaryClient.alter_wm_pool(wmAlterPoolRequest)).thenReturn(wmAlterPoolResponse);
    WMAlterPoolResponse result = handler.alter_wm_pool(wmAlterPoolRequest);
    assertThat(result, is(wmAlterPoolResponse));
    verify(primaryClient, times(1)).alter_wm_pool(wmAlterPoolRequest);
  }

  @Test
  public void drop_wm_pool() throws TException {
    WMDropPoolRequest wmDropPoolRequest = new WMDropPoolRequest();
    WMDropPoolResponse wmDropPoolResponse = new WMDropPoolResponse();

    when(primaryClient.drop_wm_pool(wmDropPoolRequest)).thenReturn(wmDropPoolResponse);
    WMDropPoolResponse result = handler.drop_wm_pool(wmDropPoolRequest);
    assertThat(result, is(wmDropPoolResponse));
    verify(primaryClient, times(1)).drop_wm_pool(wmDropPoolRequest);
  }

  @Test
  public void create_or_update_wm_mapping() throws TException {
    WMCreateOrUpdateMappingRequest wmCreateOrUpdateMappingRequest = new WMCreateOrUpdateMappingRequest();
    WMCreateOrUpdateMappingResponse wmCreateOrUpdateMappingResponse = new WMCreateOrUpdateMappingResponse();

    when(primaryClient.create_or_update_wm_mapping(wmCreateOrUpdateMappingRequest)).thenReturn(wmCreateOrUpdateMappingResponse);
    WMCreateOrUpdateMappingResponse result = handler.create_or_update_wm_mapping(wmCreateOrUpdateMappingRequest);
    assertThat(result, is(wmCreateOrUpdateMappingResponse));
    verify(primaryClient, times(1)).create_or_update_wm_mapping(wmCreateOrUpdateMappingRequest);
  }

  @Test
  public void drop_wm_mapping() throws TException {
    WMDropMappingRequest wmDropMappingRequest = new WMDropMappingRequest();
    WMDropMappingResponse wmDropMappingResponse = new WMDropMappingResponse();

    when(primaryClient.drop_wm_mapping(wmDropMappingRequest)).thenReturn(wmDropMappingResponse);
    WMDropMappingResponse result = handler.drop_wm_mapping(wmDropMappingRequest);
    assertThat(result, is(wmDropMappingResponse));
    verify(primaryClient, times(1)).drop_wm_mapping(wmDropMappingRequest);
  }

  @Test
  public void create_or_drop_wm_trigger_to_pool_mapping() throws TException {
    WMCreateOrDropTriggerToPoolMappingRequest wmCreateOrDropTriggerToPoolMappingRequest = new WMCreateOrDropTriggerToPoolMappingRequest();
    WMCreateOrDropTriggerToPoolMappingResponse wmCreateOrDropTriggerToPoolMappingResponse = new WMCreateOrDropTriggerToPoolMappingResponse();

    when(primaryClient.create_or_drop_wm_trigger_to_pool_mapping(wmCreateOrDropTriggerToPoolMappingRequest)).thenReturn(wmCreateOrDropTriggerToPoolMappingResponse);
    WMCreateOrDropTriggerToPoolMappingResponse result = handler.create_or_drop_wm_trigger_to_pool_mapping(wmCreateOrDropTriggerToPoolMappingRequest);
    assertThat(result, is(wmCreateOrDropTriggerToPoolMappingResponse));
    verify(primaryClient, times(1)).create_or_drop_wm_trigger_to_pool_mapping(wmCreateOrDropTriggerToPoolMappingRequest);
  }

  @Test
  public void add_schema_version() throws TException {
    ISchemaName iSchemaName = new ISchemaName();
    iSchemaName.setDbName(DB_P);
    iSchemaName.setCatName(CAT_1);
    iSchemaName.setSchemaName(SCH_1);

    SchemaVersion schemaVersion = new SchemaVersion();
    schemaVersion.setSchema(iSchemaName);

    when(primaryMapping.transformInboundSchemaVersion(schemaVersion)).thenReturn(schemaVersion);
    doNothing().when(primaryClient).add_schema_version(isA(SchemaVersion.class));
    handler.add_schema_version(schemaVersion);
    verify(primaryClient, times(1)).add_schema_version(schemaVersion);
  }

  @Test
  public void get_schema_latest_version() throws TException {
    ISchemaName iSchemaName = new ISchemaName();
    iSchemaName.setDbName(DB_P);
    iSchemaName.setCatName(CAT_1);
    iSchemaName.setSchemaName(SCH_1);

    SchemaVersion schemaVersion = new SchemaVersion();
    schemaVersion.setSchema(iSchemaName);

    SchemaVersion outboundSchemaVersion = new SchemaVersion();
    outboundSchemaVersion.setSchema(iSchemaName);

    when(primaryMapping.transformInboundISchemaName(iSchemaName)).thenReturn(iSchemaName);
    when(primaryClient.get_schema_latest_version(iSchemaName)).thenReturn(schemaVersion);
    when(primaryMapping.transformOutboundSchemaVersion(schemaVersion)).thenReturn(outboundSchemaVersion);

    SchemaVersion result = handler.get_schema_latest_version(iSchemaName);
    assertThat(result, is(outboundSchemaVersion));
    verify(primaryClient, times(1)).get_schema_latest_version(iSchemaName);
  }

  @Test
  public void get_schema_all_versions() throws TException {
    ISchemaName iSchemaName = new ISchemaName();
    iSchemaName.setDbName(DB_P);
    iSchemaName.setCatName(CAT_1);
    iSchemaName.setSchemaName(SCH_1);

    SchemaVersion schemaVersion = new SchemaVersion();
    schemaVersion.setSchema(iSchemaName);

    SchemaVersion outboundSchemaVersion = new SchemaVersion();
    outboundSchemaVersion.setSchema(iSchemaName);

    when(primaryClient.get_schema_all_versions(iSchemaName)).thenReturn(Lists.newArrayList(schemaVersion));
    when(primaryMapping.transformInboundISchemaName(iSchemaName)).thenReturn(iSchemaName);
    when(primaryMapping.transformOutboundSchemaVersions(Lists.newArrayList(schemaVersion))).thenReturn(Lists.newArrayList(outboundSchemaVersion));
    List<SchemaVersion> result = handler.get_schema_all_versions(iSchemaName);
    assertThat(result, is(Lists.newArrayList(Lists.newArrayList(schemaVersion))));
    verify(primaryClient, times(1)).get_schema_all_versions(iSchemaName);
  }

  @Test
  public void drop_schema_version() throws TException {
    SchemaVersionDescriptor schemaVersionDescriptor = new SchemaVersionDescriptor();
    ISchemaName iSchemaName = new ISchemaName();
    iSchemaName.setDbName(DB_P);
    iSchemaName.setCatName(CAT_1);
    iSchemaName.setSchemaName(SCH_1);
    schemaVersionDescriptor.setSchema(iSchemaName);

    when(primaryMapping.transformInboundSchemaVersionDescriptor(schemaVersionDescriptor)).thenReturn(schemaVersionDescriptor);
    doNothing().when(primaryClient).drop_schema_version(isA(SchemaVersionDescriptor.class));
    handler.drop_schema_version(schemaVersionDescriptor);
    verify(primaryClient, times(1)).drop_schema_version(schemaVersionDescriptor);
  }

  @Test
  public void get_schemas_by_cols() throws TException {
    FindSchemasByColsRqst findSchemasByColsRqst = new FindSchemasByColsRqst();
    FindSchemasByColsResp findSchemasByColsResp = new FindSchemasByColsResp();

    when(primaryMapping.transformOutboundFindSchemasByColsResp(findSchemasByColsResp)).thenReturn(findSchemasByColsResp);
    when(primaryClient.get_schemas_by_cols(findSchemasByColsRqst)).thenReturn(findSchemasByColsResp);
    FindSchemasByColsResp result = handler.get_schemas_by_cols(findSchemasByColsRqst);
    assertThat(result, is(findSchemasByColsResp));
    verify(primaryClient, times(1)).get_schemas_by_cols(findSchemasByColsRqst);
  }

  @Test
  public void map_schema_version_to_serde() throws TException {
    MapSchemaVersionToSerdeRequest mapSchemaVersionToSerdeRequest = new MapSchemaVersionToSerdeRequest();

    when(primaryMapping.transformInboundMapSchemaVersionToSerdeRequest(mapSchemaVersionToSerdeRequest)).
            thenReturn(new MapSchemaVersionToSerdeRequest());

    doNothing().when(primaryClient).map_schema_version_to_serde(isA(MapSchemaVersionToSerdeRequest.class));
    handler.map_schema_version_to_serde(mapSchemaVersionToSerdeRequest);
    verify(primaryClient, times(1)).map_schema_version_to_serde(mapSchemaVersionToSerdeRequest);
  }

  @Test
  public void set_schema_version_state() throws TException {
    SetSchemaVersionStateRequest setSchemaVersionStateRequest = new SetSchemaVersionStateRequest();
    doNothing().when(primaryClient).set_schema_version_state(isA(SetSchemaVersionStateRequest.class));
    when(primaryMapping.transformInboundSetSchemaVersionStateRequest(setSchemaVersionStateRequest)).
            thenReturn(new SetSchemaVersionStateRequest());
    handler.set_schema_version_state(setSchemaVersionStateRequest);
    verify(primaryClient, times(1)).set_schema_version_state(setSchemaVersionStateRequest);
  }

  @Test
  public void add_serde() throws TException {
    SerDeInfo serDeInfo = new SerDeInfo();
    doNothing().when(primaryClient).add_serde(isA(SerDeInfo.class));
    handler.add_serde(serDeInfo);
    verify(primaryClient, times(1)).add_serde(serDeInfo);
  }

  @Test
  public void get_serde() throws TException {
    GetSerdeRequest serdeRequest = new GetSerdeRequest();
    serdeRequest.setSerdeName("serdeName");
    serdeRequest.setFieldValue(GetSerdeRequest._Fields.SERDE_NAME, "serdeName");

    SerDeInfo serDeInfo = new SerDeInfo();

    when(primaryClient.get_serde(serdeRequest)).thenReturn(serDeInfo);
    SerDeInfo result = handler.get_serde(serdeRequest);
    assertThat(result, is(serDeInfo));
  }

  @Test
  public void get_lock_materialization_rebuild() throws TException {
    LockResponse lockResponse = new LockResponse();
    lockResponse.setFieldValue(LockResponse._Fields.LOCKID, 1000L);

    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn(DB_P);
    when(primaryClient.get_lock_materialization_rebuild(DB_P, CAT_1, 1000L)).thenReturn(lockResponse);
    LockResponse result = handler.get_lock_materialization_rebuild(DB_P, CAT_1, 1000L);
    assertThat(result, is(lockResponse));
  }

  @Test
  public void heartbeat_lock_materialization_rebuild() throws TException {
    when(primaryMapping.transformInboundDatabaseName(DB_P)).thenReturn(DB_P);
    when(primaryClient.heartbeat_lock_materialization_rebuild(DB_P, CAT_1, 1000L)).thenReturn(true);
    boolean result = handler.heartbeat_lock_materialization_rebuild(DB_P, CAT_1, 1000L);
    assertThat(result, is(true));
  }

  @Test
  public void add_runtime_stats() throws TException {
    RuntimeStat runtimeStat = new RuntimeStat();
    runtimeStat.setFieldValue( RuntimeStat._Fields.PAYLOAD, ByteBuffer.allocate(10));

    handler.add_runtime_stats(runtimeStat);
    verify(primaryClient).add_runtime_stats(runtimeStat);
  }

  @Test
  public void get_runtime_stats() throws TException {
    GetRuntimeStatsRequest getRuntimeStatsRequest = new GetRuntimeStatsRequest();
    getRuntimeStatsRequest.setFieldValue( MAX_WEIGHT, 1);

    List<RuntimeStat> runtimeStatList = Lists.newArrayList(new RuntimeStat());

    when(primaryClient.get_runtime_stats(getRuntimeStatsRequest)).thenReturn(runtimeStatList);
    List<RuntimeStat> result = handler.get_runtime_stats(getRuntimeStatsRequest);
    assertThat(result, is(runtimeStatList));
  }

  @Test
  public void cm_recycle() throws TException {
    CmRecycleRequest cmRecycleRequest = new CmRecycleRequest();
    cmRecycleRequest.setFieldValue( DATA_PATH, "test");

    CmRecycleResponse cmRecycleResponse = new CmRecycleResponse();

    when(primaryClient.cm_recycle(cmRecycleRequest)).thenReturn(cmRecycleResponse);
    CmRecycleResponse result = handler.cm_recycle(cmRecycleRequest);
    assertThat(result, is(cmRecycleResponse));
  }

  @Test
  public void get_notification_events_count() throws TException {
    NotificationEventsCountRequest notificationEventsCountRequest = new NotificationEventsCountRequest();
    notificationEventsCountRequest.setDbName(DB_P);
    notificationEventsCountRequest.setCatName(CAT_1);

    NotificationEventsCountResponse notificationEventsCountResponse = new NotificationEventsCountResponse();
    notificationEventsCountResponse.setEventsCount(10);

    when(primaryMapping.transformInboundNotificationEventsCountRequest(notificationEventsCountRequest)).thenReturn(notificationEventsCountRequest);
    when(primaryClient.get_notification_events_count(notificationEventsCountRequest)).thenReturn(notificationEventsCountResponse);
    NotificationEventsCountResponse result = handler.get_notification_events_count(notificationEventsCountRequest);
    assertThat(result, is(notificationEventsCountResponse));
  }

  @Test
  public void get_unique_constraints() throws TException {
    UniqueConstraintsRequest uniqueConstraintsRequest = new UniqueConstraintsRequest();
    uniqueConstraintsRequest.setDb_name(DB_P);
    uniqueConstraintsRequest.setTbl_name(TBL_1);
    uniqueConstraintsRequest.setCatName(CAT_1);

    UniqueConstraintsResponse uniqueConstraintsResponse = new UniqueConstraintsResponse();
    uniqueConstraintsResponse.setUniqueConstraints(Lists.newArrayList(new SQLUniqueConstraint()));

    when(primaryMapping.transformInboundUniqueConstraintsRequest(uniqueConstraintsRequest)).
            thenReturn(uniqueConstraintsRequest);
    when(primaryMapping.transformOutboundUniqueConstraintsResponse(uniqueConstraintsResponse)).
            thenReturn(uniqueConstraintsResponse);

    when(primaryClient.get_unique_constraints(uniqueConstraintsRequest)).thenReturn(uniqueConstraintsResponse);
    UniqueConstraintsResponse result = handler.get_unique_constraints(uniqueConstraintsRequest);
    assertThat(result, is(uniqueConstraintsResponse));
  }

  @Test
  public void get_not_null_constraints() throws TException {
    NotNullConstraintsRequest notNullConstraintsRequest = new NotNullConstraintsRequest();
    notNullConstraintsRequest.setDb_name(DB_P);
    notNullConstraintsRequest.setTbl_name(TBL_1);
    notNullConstraintsRequest.setCatName(CAT_1);

    NotNullConstraintsResponse notNullConstraintsResponse = new NotNullConstraintsResponse();
    notNullConstraintsResponse.setNotNullConstraints(Lists.newArrayList(new SQLNotNullConstraint()));

    when(primaryMapping.transformInboundNotNullConstraintsRequest(notNullConstraintsRequest)).thenReturn(notNullConstraintsRequest);
    when(primaryMapping.transformOutboundNotNullConstraintsResponse(notNullConstraintsResponse)).thenReturn(notNullConstraintsResponse);

    when(primaryClient.get_not_null_constraints(notNullConstraintsRequest)).thenReturn(notNullConstraintsResponse);
    NotNullConstraintsResponse result = handler.get_not_null_constraints(notNullConstraintsRequest);
    assertThat(result, is(notNullConstraintsResponse));
  }

  @Test
  public void get_default_constraints() throws TException {
    DefaultConstraintsRequest defaultConstraintsRequest = new DefaultConstraintsRequest();
    defaultConstraintsRequest.setDb_name(DB_P);
    defaultConstraintsRequest.setTbl_name(TBL_1);
    defaultConstraintsRequest.setCatName(CAT_1);

    DefaultConstraintsResponse defaultConstraintsResponse = new DefaultConstraintsResponse();
    defaultConstraintsResponse.setDefaultConstraints(Lists.newArrayList(new SQLDefaultConstraint()));

    when(primaryMapping.transformInboundDefaultConstraintsRequest(defaultConstraintsRequest)).
            thenReturn(defaultConstraintsRequest);
    when(primaryMapping.transformOutboundDefaultConstraintsResponse(defaultConstraintsResponse)).
            thenReturn(defaultConstraintsResponse);

    when(primaryClient.get_default_constraints(defaultConstraintsRequest)).thenReturn(defaultConstraintsResponse);
    DefaultConstraintsResponse result = handler.get_default_constraints(defaultConstraintsRequest);
    assertThat(result, is(defaultConstraintsResponse));
  }

  @Test
  public void get_check_constraints() throws TException {
    CheckConstraintsRequest checkConstraintsRequest = new CheckConstraintsRequest();
    checkConstraintsRequest.setDb_name(DB_P);
    checkConstraintsRequest.setTbl_name(TBL_1);
    checkConstraintsRequest.setCatName(CAT_1);

    CheckConstraintsResponse checkConstraintsResponse = new CheckConstraintsResponse();
    checkConstraintsResponse.setCheckConstraints(Lists.newArrayList(new SQLCheckConstraint()));

    when(primaryMapping.transformInboundCheckConstraintsRequest(checkConstraintsRequest)).thenReturn(checkConstraintsRequest);
    when(primaryMapping.transformOutboundCheckConstraintsResponse(checkConstraintsResponse)).thenReturn(checkConstraintsResponse);

    when(primaryClient.get_check_constraints(checkConstraintsRequest)).thenReturn(checkConstraintsResponse);
    CheckConstraintsResponse result = handler.get_check_constraints(checkConstraintsRequest);
    assertThat(result, is(checkConstraintsResponse));
  }

  @Test
  public void get_materialized_views_for_rewriting() throws TException {
    String dbName = DB_P;
    List<String> expected = Arrays.asList("view1", "view2");
    when(primaryClient.get_materialized_views_for_rewriting(dbName)).thenReturn(expected);
    List<String> result = handler.get_materialized_views_for_rewriting(dbName);
    assertThat(result, is(expected));
  }

  @Test
  public void get_materialization_invalidation_info() throws TException {
    CreationMetadata request = new CreationMetadata();
    request.setDbName(DB_P);
    when(primaryMapping.transformInboundCreationMetadata(request)).thenReturn(request);
    handler.get_materialization_invalidation_info(request, "dummy");
    verify(primaryClient).get_materialization_invalidation_info(request, "dummy");
  }

  @Test
  public void update_creation_metadata() throws TException {
    CreationMetadata request = new CreationMetadata();
    handler.update_creation_metadata(CAT_1, DB_P, TBL_1, request);
    verify(primaryClient).update_creation_metadata(CAT_1, DB_P, TBL_1, request);
  }
}
