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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.Warehouse;
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
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.AlterCatalogRequest;
import org.apache.hadoop.hive.metastore.api.AlterISchemaRequest;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreateCatalogRequest;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
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
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
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
import org.apache.hadoop.hive.metastore.api.GrantRevokeRoleResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MapSchemaVersionToSerdeRequest;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
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
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsResult;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PutFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.PutFileMetadataResult;
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
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.TableStatsResult;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
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
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import lombok.extern.log4j.Log4j2;

import com.facebook.fb303.FacebookBase;
import com.facebook.fb303.fb_status;
import com.jcabi.aspects.Loggable;

import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.impl.NotifyingFederationService;
import com.hotels.bdp.waggledance.metrics.Monitored;

@Monitored
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Log4j2
class FederatedHMSHandler extends FacebookBase implements CloseableIHMSHandler {

  private static final String INVOCATION_LOG_NAME = "com.hotels.bdp.waggledance.server.invocation-log";
  private final MappingEventListener databaseMappingService;
  private final NotifyingFederationService notifyingFederationService;
  private final WaggleDanceConfiguration waggleDanceConfiguration;
  private Configuration conf;
  private SaslServerWrapper saslServerWrapper;

  FederatedHMSHandler(
      MappingEventListener databaseMappingService,
      NotifyingFederationService notifyingFederationService,
      WaggleDanceConfiguration waggleDanceConfiguration,
      SaslServerWrapper saslServerWrapper) {
    super("waggle-dance-handler");
    this.databaseMappingService = databaseMappingService;
    this.notifyingFederationService = notifyingFederationService;
    this.waggleDanceConfiguration = waggleDanceConfiguration;
    this.notifyingFederationService.subscribe(databaseMappingService);
    this.saslServerWrapper= saslServerWrapper;
  }

  private ThriftHiveMetastore.Iface getPrimaryClient() throws TException {
    return databaseMappingService.primaryDatabaseMapping().getClient();
  }

  private DatabaseMapping checkWritePermissions(String databaseName) throws TException {
    DatabaseMapping mapping = databaseMappingService.databaseMapping(databaseName);
    mapping.checkWritePermissions(databaseName);
    return mapping;
  }

  private DatabaseMapping getDbMappingAndCheckTableAllowed(String dbName, String tblName) throws NoSuchObjectException {
    DatabaseMapping mapping = databaseMappingService.databaseMapping(dbName);
    databaseMappingService.checkTableAllowed(dbName, tblName, mapping);
    return mapping;
  }

  private DatabaseMapping checkWritePermissionsAndCheckTableAllowed(String dbName, String tblName) throws TException {
    DatabaseMapping mapping = checkWritePermissions(dbName);
    databaseMappingService.checkTableAllowed(dbName, tblName, mapping);
    return mapping;
  }

  private void checkWritePermissionsAndCheckTableAllowed(String dest_db, String dest_table_name, DatabaseMapping mapping)
      throws NoSuchObjectException {
    mapping.checkWritePermissions(dest_db);
    databaseMappingService.checkTableAllowed(dest_db, dest_table_name, mapping);
  }

  @Override
  public void close() throws IOException {
    shutdown();
  }

  @Override
  public void shutdown() {
    super.shutdown();
    try {
      notifyingFederationService.unsubscribe(databaseMappingService);
      databaseMappingService.close();
    } catch (IOException e) {
      log.warn("Error shutting down federated handler", e);
    }
  }

  //////////////////////////////

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public String getMetaConf(String key) throws MetaException, TException {
    return getPrimaryClient().getMetaConf(key);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void setMetaConf(String key, String value) throws MetaException, TException {
    getPrimaryClient().setMetaConf(key, value);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void create_catalog(CreateCatalogRequest createCatalogRequest) throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    databaseMapping.getClient().create_catalog(createCatalogRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void alter_catalog(AlterCatalogRequest alterCatalogRequest) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    databaseMapping.getClient().alter_catalog(alterCatalogRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GetCatalogResponse get_catalog(GetCatalogRequest getCatalogRequest) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    return databaseMapping.getClient().get_catalog(getCatalogRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GetCatalogsResponse get_catalogs() throws MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    return databaseMapping.getClient().get_catalogs();
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void drop_catalog(DropCatalogRequest dropCatalogRequest) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    databaseMapping.getClient().drop_catalog(dropCatalogRequest);
  }


  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void create_database(Database database)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    DatabaseMapping mapping = databaseMappingService.primaryDatabaseMapping();
    mapping.createDatabase(mapping.transformInboundDatabase(database));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Database get_database(String name) throws NoSuchObjectException, MetaException, TException {
    log.info("Fetching database {}", name);
    DatabaseMapping mapping = databaseMappingService.databaseMapping(name);
    log.info("Mapping is '{}'", mapping.getDatabasePrefix());
    Database result = mapping.getClient().get_database(mapping.transformInboundDatabaseName(name));
    return mapping.transformOutboundDatabase(mapping.getMetastoreFilter().filterDatabase(result));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void drop_database(String name, boolean deleteData, boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissions(name);
    mapping.getClient().drop_database(mapping.transformInboundDatabaseName(name), deleteData, cascade);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> get_databases(String pattern) throws MetaException, TException {
    return databaseMappingService.getPanopticOperationHandler().getAllDatabases(pattern);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> get_all_databases() throws MetaException, TException {
    return databaseMappingService.getPanopticOperationHandler().getAllDatabases();
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void alter_database(String dbname, Database db) throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = checkWritePermissions(dbname);
    mapping.checkWritePermissions(db.getName());
    mapping
        .getClient()
        .alter_database(mapping.transformInboundDatabaseName(dbname), mapping.transformInboundDatabase(db));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Type get_type(String name) throws MetaException, NoSuchObjectException, TException {
    return getPrimaryClient().get_type(name);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean create_type(Type type)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    return getPrimaryClient().create_type(type);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean drop_type(String type) throws MetaException, NoSuchObjectException, TException {
    return getPrimaryClient().drop_type(type);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Map<String, Type> get_type_all(String name) throws MetaException, TException {
    return getPrimaryClient().get_type_all(name);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<FieldSchema> get_fields(String db_name, String table_name)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, table_name);
    return mapping.getClient().get_fields(mapping.transformInboundDatabaseName(db_name), table_name);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<FieldSchema> get_schema(String db_name, String table_name)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, table_name);
    return mapping.getClient().get_schema(mapping.transformInboundDatabaseName(db_name), table_name);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void create_table(Table tbl)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = checkWritePermissions(tbl.getDbName());
    mapping.getClient().create_table(mapping.transformInboundTable(tbl));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void create_table_with_environment_context(Table tbl, EnvironmentContext environment_context)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = checkWritePermissions(tbl.getDbName());
    mapping.getClient().create_table_with_environment_context(mapping.transformInboundTable(tbl), environment_context);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void create_table_with_constraints(Table tbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
                                            List<SQLUniqueConstraint> uniqueConstraints, List<SQLNotNullConstraint> notNullConstraints,
                                            List<SQLDefaultConstraint> defaultConstraints, List<SQLCheckConstraint> checkConstraints)
          throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    DatabaseMapping databaseMapping = checkWritePermissions(tbl.getDbName());
    databaseMapping.getClient().create_table_with_constraints(databaseMapping.transformInboundTable(tbl),
            databaseMapping.transformInboundSQLPrimaryKeys(primaryKeys),
            databaseMapping.transformInboundSQLForeignKeys(foreignKeys),
            databaseMapping.transformInboundSQLUniqueConstraints(uniqueConstraints),
            databaseMapping.transformInboundSQLNotNullConstraints(notNullConstraints),
            databaseMapping.transformInboundSQLDefaultConstraints(defaultConstraints),
            databaseMapping.transformInboundSQLCheckConstraints(checkConstraints));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void drop_table(String dbname, String name, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(dbname, name);
    mapping.getClient().drop_table(mapping.transformInboundDatabaseName(dbname), name, deleteData);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void drop_table_with_environment_context(
      String dbname,
      String name,
      boolean deleteData,
      EnvironmentContext environment_context)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(dbname, name);
    mapping
        .getClient()
        .drop_table_with_environment_context(mapping.transformInboundDatabaseName(dbname), name, deleteData,
            environment_context);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void truncate_table(String dbName, String tableName, List<String> partNames) throws MetaException, TException {
    DatabaseMapping databaseMapping = getDbMappingAndCheckTableAllowed(dbName, tableName);
    databaseMapping.getClient().truncate_table(databaseMapping.transformInboundDatabaseName(dbName), tableName, partNames);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> get_tables(String db_name, String pattern) throws MetaException, TException {
    DatabaseMapping mapping = databaseMappingService.databaseMapping(db_name);
    List<String> resultTables = mapping.getClient().get_tables(mapping.transformInboundDatabaseName(db_name), pattern);
    resultTables = databaseMappingService.filterTables(db_name, resultTables, mapping);
    return mapping.getMetastoreFilter().filterTableNames(null, db_name, resultTables);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> get_all_tables(String db_name) throws MetaException, TException {
    DatabaseMapping mapping = databaseMappingService.databaseMapping(db_name);
    List<String> resultTables =  mapping.getClient().get_all_tables(mapping.transformInboundDatabaseName(db_name));
    resultTables = databaseMappingService.filterTables(db_name, resultTables, mapping);
    return mapping.getMetastoreFilter().filterTableNames(null, db_name, resultTables);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Table get_table(String dbname, String tbl_name) throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(dbname, tbl_name);
    Table table = mapping.getClient().get_table(mapping.transformInboundDatabaseName(dbname), tbl_name);
    return mapping
        .transformOutboundTable(mapping.getMetastoreFilter().filterTable(table));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<Table> get_table_objects_by_name(String dbname, List<String> tbl_names)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    DatabaseMapping mapping = databaseMappingService.databaseMapping(dbname);
    List<String> filteredTables = databaseMappingService.filterTables(dbname, tbl_names, mapping);
    List<Table> tables = mapping
        .getClient()
        .get_table_objects_by_name(mapping.transformInboundDatabaseName(dbname), filteredTables);
    tables = mapping.getMetastoreFilter().filterTables(tables);
    List<Table> outboundTables = new ArrayList<>(tables.size());
    for (Table table : tables) {
      outboundTables.add(mapping.transformOutboundTable(table));
    }
    return outboundTables;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> get_table_names_by_filter(String dbname, String filter, short max_tables)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    DatabaseMapping mapping = databaseMappingService.databaseMapping(dbname);
    List<String> resultTables = mapping.getClient()
        .get_table_names_by_filter(mapping.transformInboundDatabaseName(dbname), filter, max_tables);
    List<String> result = databaseMappingService.filterTables(dbname, resultTables, mapping);
    return mapping.getMetastoreFilter().filterTableNames(null, dbname, result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void alter_table(String dbname, String tbl_name, Table new_tbl)
      throws InvalidOperationException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(dbname, tbl_name);
    mapping.checkWritePermissions(new_tbl.getDbName());
    databaseMappingService.checkTableAllowed(new_tbl.getDbName(), new_tbl.getTableName(), mapping);
    mapping
        .getClient()
        .alter_table(mapping.transformInboundDatabaseName(dbname), tbl_name, mapping.transformInboundTable(new_tbl));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void alter_table_with_environment_context(
      String dbname,
      String tbl_name,
      Table new_tbl,
      EnvironmentContext environment_context)
      throws InvalidOperationException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(dbname, tbl_name);
    checkWritePermissionsAndCheckTableAllowed(new_tbl.getDbName(), new_tbl.getTableName(), mapping);
    mapping
        .getClient()
        .alter_table_with_environment_context(mapping.transformInboundDatabaseName(dbname), tbl_name,
            mapping.transformInboundTable(new_tbl), environment_context);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Partition add_partition(Partition new_part)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(new_part.getDbName(), new_part.getTableName());
    Partition result = mapping.getClient().add_partition(mapping.transformInboundPartition(new_part));
    return mapping.transformOutboundPartition(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Partition add_partition_with_environment_context(Partition new_part, EnvironmentContext environment_context)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(new_part.getDbName(), new_part.getTableName());
    Partition result = mapping
        .getClient()
        .add_partition_with_environment_context(mapping.transformInboundPartition(new_part), environment_context);
    return mapping.transformOutboundPartition(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public int add_partitions(List<Partition> new_parts)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    if (!new_parts.isEmpty()) {
      // Need to pick one mapping and use that for permissions and getting the client.
      // If the partitions added are for different databases in different clients that won't work with waggle-dance
      DatabaseMapping mapping = databaseMappingService.databaseMapping(new_parts.get(0).getDbName());
      for (Partition partition : new_parts) {
        checkWritePermissionsAndCheckTableAllowed(partition.getDbName(), partition.getTableName(), mapping);
      }
      return mapping.getClient().add_partitions(mapping.transformInboundPartitions(new_parts));
    }
    return 0;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public int add_partitions_pspec(List<PartitionSpec> new_parts)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    if (!new_parts.isEmpty()) {
      // Need to pick one mapping and use that for permissions and getting the client.
      // If the partitions added are for different databases in different clients that won't work with waggle-dance
      DatabaseMapping mapping = databaseMappingService.databaseMapping(new_parts.get(0).getDbName());
      for (PartitionSpec partitionSpec : new_parts) {
        checkWritePermissionsAndCheckTableAllowed(partitionSpec.getDbName(), partitionSpec.getTableName(), mapping);
      }
      return mapping.getClient().add_partitions_pspec(mapping.transformInboundPartitionSpecs(new_parts));
    }
    return 0;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Partition append_partition(String db_name, String tbl_name, List<String> part_vals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    Partition result = mapping
        .getClient()
        .append_partition(mapping.transformInboundDatabaseName(db_name), tbl_name, part_vals);
    return mapping.transformOutboundPartition(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public AddPartitionsResult add_partitions_req(AddPartitionsRequest request)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(request.getDbName(), request.getTblName());
    for (Partition partition : request.getParts()) {
      checkWritePermissionsAndCheckTableAllowed(partition.getDbName(), partition.getTableName(), mapping);
    }
    AddPartitionsResult result = mapping
        .getClient()
        .add_partitions_req(mapping.transformInboundAddPartitionsRequest(request));
    result.setPartitions(mapping.getMetastoreFilter().filterPartitions(result.getPartitions()));
    return mapping.transformOutboundAddPartitionsResult(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Partition append_partition_with_environment_context(
      String db_name,
      String tbl_name,
      List<String> part_vals,
      EnvironmentContext environment_context)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    Partition partition = mapping
        .getClient()
        .append_partition_with_environment_context(mapping.transformInboundDatabaseName(db_name), tbl_name, part_vals,
            environment_context);
    return mapping.transformOutboundPartition(partition);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Partition append_partition_by_name(String db_name, String tbl_name, String part_name)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    Partition partition = mapping
        .getClient()
        .append_partition_by_name(mapping.transformInboundDatabaseName(db_name), tbl_name, part_name);
    return mapping.transformOutboundPartition(partition);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Partition append_partition_by_name_with_environment_context(
      String db_name,
      String tbl_name,
      String part_name,
      EnvironmentContext environment_context)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    Partition partition = mapping
        .getClient()
        .append_partition_by_name_with_environment_context(mapping.transformInboundDatabaseName(db_name), tbl_name,
            part_name, environment_context);
    return mapping.transformOutboundPartition(partition);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean drop_partition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    return mapping
        .getClient()
        .drop_partition(mapping.transformInboundDatabaseName(db_name), tbl_name, part_vals, deleteData);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean drop_partition_with_environment_context(
      String db_name,
      String tbl_name,
      List<String> part_vals,
      boolean deleteData,
      EnvironmentContext environment_context)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    return mapping
        .getClient()
        .drop_partition_with_environment_context(mapping.transformInboundDatabaseName(db_name), tbl_name, part_vals,
            deleteData, environment_context);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean drop_partition_by_name(String db_name, String tbl_name, String part_name, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    return mapping
        .getClient()
        .drop_partition_by_name(mapping.transformInboundDatabaseName(db_name), tbl_name, part_name, deleteData);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean drop_partition_by_name_with_environment_context(
      String db_name,
      String tbl_name,
      String part_name,
      boolean deleteData,
      EnvironmentContext environment_context)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    return mapping
        .getClient()
        .drop_partition_by_name_with_environment_context(mapping.transformInboundDatabaseName(db_name), tbl_name,
            part_name, deleteData, environment_context);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public DropPartitionsResult drop_partitions_req(DropPartitionsRequest req)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(req.getDbName(), req.getTblName());
    DropPartitionsResult result = mapping
        .getClient()
        .drop_partitions_req(mapping.transformInboundDropPartitionRequest(req));
    return mapping.transformOutboundDropPartitionsResult(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Partition get_partition(String db_name, String tbl_name, List<String> part_vals)
      throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    Partition partition = mapping.getClient().get_partition(mapping.transformInboundDatabaseName(db_name), tbl_name, part_vals);
    return mapping
        .transformOutboundPartition(mapping.getMetastoreFilter().filterPartition(partition));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Partition exchange_partition(
      Map<String, String> partitionSpecs,
      String source_db,
      String source_table_name,
      String dest_db,
      String dest_table_name)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(source_db, source_table_name);
    checkWritePermissionsAndCheckTableAllowed(dest_db, dest_table_name, mapping);
    Partition result = mapping
        .getClient()
        .exchange_partition(partitionSpecs, mapping.transformInboundDatabaseName(source_db), source_table_name,
            mapping.transformInboundDatabaseName(dest_db), dest_table_name);
    return mapping.transformOutboundPartition(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Partition get_partition_with_auth(
      String db_name,
      String tbl_name,
      List<String> part_vals,
      String user_name,
      List<String> group_names)
      throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    Partition partition = mapping
        .getClient()
        .get_partition_with_auth(mapping.transformInboundDatabaseName(db_name), tbl_name, part_vals, user_name,
            group_names);
    return mapping.transformOutboundPartition(mapping.getMetastoreFilter().filterPartition(partition));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Partition get_partition_by_name(String db_name, String tbl_name, String part_name)
      throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    Partition partition = mapping
        .getClient()
        .get_partition_by_name(mapping.transformInboundDatabaseName(db_name), tbl_name, part_name);
    return mapping.transformOutboundPartition(mapping.getMetastoreFilter().filterPartition(partition));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME, prepend=true)
  public List<Partition> get_partitions(String db_name, String tbl_name, short max_parts)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    List<Partition> partitions = mapping
        .getClient()
        .get_partitions(mapping.transformInboundDatabaseName(db_name), tbl_name, max_parts);
    return mapping.transformOutboundPartitions(mapping.getMetastoreFilter().filterPartitions(partitions));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME, prepend=true)
  public List<Partition> get_partitions_with_auth(
      String db_name,
      String tbl_name,
      short max_parts,
      String user_name,
      List<String> group_names)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    List<Partition> partitions = mapping
        .getClient()
        .get_partitions_with_auth(mapping.transformInboundDatabaseName(db_name), tbl_name, max_parts, user_name,
            group_names);
    return mapping.transformOutboundPartitions(mapping.getMetastoreFilter().filterPartitions(partitions));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME, prepend=true)
  public List<PartitionSpec> get_partitions_pspec(String db_name, String tbl_name, int max_parts)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    List<PartitionSpec> partitionSpecs = mapping
        .getClient()
        .get_partitions_pspec(mapping.transformInboundDatabaseName(db_name), tbl_name, max_parts);
    return mapping.transformOutboundPartitionSpecs(mapping.getMetastoreFilter().filterPartitionSpecs(partitionSpecs));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> get_partition_names(String db_name, String tbl_name, short max_parts)
      throws MetaException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    List<String> result = mapping.getClient()
        .get_partition_names(mapping.transformInboundDatabaseName(db_name), tbl_name, max_parts);
    return mapping.getMetastoreFilter().filterPartitionNames(null, db_name, tbl_name, result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME, prepend=true)
  public List<Partition> get_partitions_ps(String db_name, String tbl_name, List<String> part_vals, short max_parts)
      throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    List<Partition> partitions = mapping
        .getClient()
        .get_partitions_ps(mapping.transformInboundDatabaseName(db_name), tbl_name, part_vals, max_parts);
    return mapping.transformOutboundPartitions(mapping.getMetastoreFilter().filterPartitions(partitions));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME, prepend=true)
  public List<Partition> get_partitions_ps_with_auth(
      String db_name,
      String tbl_name,
      List<String> part_vals,
      short max_parts,
      String user_name,
      List<String> group_names)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    List<Partition> partitions = mapping
        .getClient()
        .get_partitions_ps_with_auth(mapping.transformInboundDatabaseName(db_name), tbl_name, part_vals, max_parts,
            user_name, group_names);
    return mapping.transformOutboundPartitions(mapping.getMetastoreFilter().filterPartitions(partitions));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> get_partition_names_ps(String db_name, String tbl_name, List<String> part_vals, short max_parts)
      throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    List<String> result = mapping
        .getClient()
        .get_partition_names_ps(mapping.transformInboundDatabaseName(db_name), tbl_name, part_vals, max_parts);
    return mapping.getMetastoreFilter().filterPartitionNames(null, db_name, tbl_name, result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME, prepend=true)
  public List<Partition> get_partitions_by_filter(String db_name, String tbl_name, String filter, short max_parts)
      throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    List<Partition> partitions = mapping
        .getClient()
        .get_partitions_by_filter(mapping.transformInboundDatabaseName(db_name), tbl_name, filter, max_parts);
    return mapping.transformOutboundPartitions(mapping.getMetastoreFilter().filterPartitions(partitions));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME, prepend=true)
  public List<PartitionSpec> get_part_specs_by_filter(String db_name, String tbl_name, String filter, int max_parts)
      throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    List<PartitionSpec> partitionSpecs = mapping
        .getClient()
        .get_part_specs_by_filter(mapping.transformInboundDatabaseName(db_name), tbl_name, filter, max_parts);
    return mapping.transformOutboundPartitionSpecs(mapping.getMetastoreFilter().filterPartitionSpecs(partitionSpecs));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public PartitionsByExprResult get_partitions_by_expr(PartitionsByExprRequest req)
      throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(req.getDbName(), req.getTblName());
    PartitionsByExprResult result = mapping
        .getClient()
        .get_partitions_by_expr(mapping.transformInboundPartitionsByExprRequest(req));
    result.setPartitions(mapping.getMetastoreFilter().filterPartitions(result.getPartitions()));
    return mapping.transformOutboundPartitionsByExprResult(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME, prepend=true)
  public List<Partition> get_partitions_by_names(String db_name, String tbl_name, List<String> names)
      throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    List<Partition> partitions = mapping
        .getClient()
        .get_partitions_by_names(mapping.transformInboundDatabaseName(db_name), tbl_name, names);
    return mapping.transformOutboundPartitions(mapping.getMetastoreFilter().filterPartitions(partitions));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void alter_partition(String db_name, String tbl_name, Partition new_part)
      throws InvalidOperationException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    checkWritePermissionsAndCheckTableAllowed(new_part.getDbName(), new_part.getTableName());
    mapping
        .getClient()
        .alter_partition(mapping.transformInboundDatabaseName(db_name), tbl_name,
            mapping.transformInboundPartition(new_part));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void alter_partitions(String db_name, String tbl_name, List<Partition> new_parts)
      throws InvalidOperationException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    for (Partition newPart : new_parts) {
      checkWritePermissionsAndCheckTableAllowed(newPart.getDbName(), newPart.getTableName(), mapping);
    }
    mapping
        .getClient()
        .alter_partitions(mapping.transformInboundDatabaseName(db_name), tbl_name,
            mapping.transformInboundPartitions(new_parts));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void alter_partition_with_environment_context(
      String db_name,
      String tbl_name,
      Partition new_part,
      EnvironmentContext environment_context)
      throws InvalidOperationException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    checkWritePermissionsAndCheckTableAllowed(new_part.getDbName(), new_part.getTableName(), mapping);
    mapping
        .getClient()
        .alter_partition_with_environment_context(mapping.transformInboundDatabaseName(db_name), tbl_name,
            mapping.transformInboundPartition(new_part), environment_context);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void rename_partition(String db_name, String tbl_name, List<String> part_vals, Partition new_part)
      throws InvalidOperationException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    checkWritePermissionsAndCheckTableAllowed(new_part.getDbName(), new_part.getTableName(), mapping);
    mapping.getClient().rename_partition(mapping.transformInboundDatabaseName(db_name), tbl_name, part_vals, new_part);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean partition_name_has_valid_characters(List<String> part_vals, boolean throw_exception)
      throws MetaException, TException {
    return getPrimaryClient().partition_name_has_valid_characters(part_vals, throw_exception);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public String get_config_value(String name, String defaultValue) throws ConfigValSecurityException, TException {
    return getPrimaryClient().get_config_value(name, defaultValue);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> partition_name_to_vals(String part_name) throws MetaException, TException {
    return getPrimaryClient().partition_name_to_vals(part_name);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Map<String, String> partition_name_to_spec(String part_name) throws MetaException, TException {
    return getPrimaryClient().partition_name_to_spec(part_name);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void markPartitionForEvent(
      String db_name,
      String tbl_name,
      Map<String, String> part_vals,
      PartitionEventType eventType)
      throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException, UnknownPartitionException,
      InvalidPartitionException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    mapping
        .getClient()
        .markPartitionForEvent(mapping.transformInboundDatabaseName(db_name), tbl_name, part_vals, eventType);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean isPartitionMarkedForEvent(
      String db_name,
      String tbl_name,
      Map<String, String> part_vals,
      PartitionEventType eventType)
      throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException, UnknownPartitionException,
      InvalidPartitionException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name,tbl_name);
    return mapping
        .getClient()
        .isPartitionMarkedForEvent(mapping.transformInboundDatabaseName(db_name), tbl_name, part_vals, eventType);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean update_table_column_statistics(ColumnStatistics stats_obj)
      throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    String dbName = stats_obj.getStatsDesc().getDbName();
    String tblName = stats_obj.getStatsDesc().getTableName();
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(dbName, tblName);
    return mapping.getClient().update_table_column_statistics(mapping.transformInboundColumnStatistics(stats_obj));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean update_partition_column_statistics(ColumnStatistics stats_obj)
      throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    DatabaseMapping mapping = checkWritePermissions(stats_obj.getStatsDesc().getDbName());
    return mapping.getClient().update_partition_column_statistics(mapping.transformInboundColumnStatistics(stats_obj));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public ColumnStatistics get_table_column_statistics(String db_name, String tbl_name, String col_name)
      throws NoSuchObjectException, MetaException, InvalidInputException, InvalidObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    ColumnStatistics result = mapping
        .getClient()
        .get_table_column_statistics(mapping.transformInboundDatabaseName(db_name), tbl_name, col_name);
    return mapping.transformOutboundColumnStatistics(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public ColumnStatistics get_partition_column_statistics(
      String db_name,
      String tbl_name,
      String part_name,
      String col_name)
      throws NoSuchObjectException, MetaException, InvalidInputException, InvalidObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    ColumnStatistics result = mapping
        .getClient()
        .get_partition_column_statistics(mapping.transformInboundDatabaseName(db_name), tbl_name, part_name, col_name);
    return mapping.transformOutboundColumnStatistics(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public TableStatsResult get_table_statistics_req(TableStatsRequest request)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(request.getDbName(), request.getTblName());
    return mapping.getClient().get_table_statistics_req(mapping.transformInboundTableStatsRequest(request));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public PartitionsStatsResult get_partitions_statistics_req(PartitionsStatsRequest request)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(request.getDbName(), request.getTblName());
    return mapping.getClient().get_partitions_statistics_req(mapping.transformInboundPartitionsStatsRequest(request));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public AggrStats get_aggr_stats_for(PartitionsStatsRequest request)
      throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(request.getDbName(), request.getTblName());
    return mapping.getClient().get_aggr_stats_for(mapping.transformInboundPartitionsStatsRequest(request));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean set_aggr_stats_for(SetPartitionsStatsRequest request)
      throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    if (!request.getColStats().isEmpty()) {
      DatabaseMapping mapping = databaseMappingService
          .databaseMapping(request.getColStats().get(0).getStatsDesc().getDbName());
      for (ColumnStatistics stats : request.getColStats()) {
        checkWritePermissionsAndCheckTableAllowed(stats.getStatsDesc().getDbName(), stats.getStatsDesc().getTableName(), mapping);
      }
      return mapping.getClient().set_aggr_stats_for(mapping.transformInboundSetPartitionStatsRequest(request));
    }
    return false;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean delete_partition_column_statistics(String db_name, String tbl_name, String part_name, String col_name)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    return mapping
        .getClient()
        .delete_partition_column_statistics(mapping.transformInboundDatabaseName(db_name), tbl_name, part_name,
            col_name);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean delete_table_column_statistics(String db_name, String tbl_name, String col_name)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    return mapping
        .getClient()
        .delete_table_column_statistics(mapping.transformInboundDatabaseName(db_name), tbl_name, col_name);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void create_function(Function func)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = checkWritePermissions(func.getDbName());
    mapping.getClient().create_function(mapping.transformInboundFunction(func));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void drop_function(String dbName, String funcName) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissions(dbName);
    mapping.getClient().drop_function(mapping.transformInboundDatabaseName(dbName), funcName);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void alter_function(String dbName, String funcName, Function newFunc)
      throws InvalidOperationException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissions(dbName);
    mapping.checkWritePermissions(newFunc.getDbName());
    mapping
        .getClient()
        .alter_function(mapping.transformInboundDatabaseName(dbName), funcName,
            mapping.transformInboundFunction(newFunc));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> get_functions(String dbName, String pattern) throws MetaException, TException {
    DatabaseMapping mapping = databaseMappingService.databaseMapping(dbName);
    return mapping.getClient().get_functions(mapping.transformInboundDatabaseName(dbName), pattern);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Function get_function(String dbName, String funcName) throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = databaseMappingService.databaseMapping(dbName);
    return mapping
        .transformOutboundFunction(
            mapping.getClient().get_function(mapping.transformInboundDatabaseName(dbName), funcName));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean create_role(Role role) throws MetaException, TException {
    return getPrimaryClient().create_role(role);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean drop_role(String role_name) throws MetaException, TException {
    return getPrimaryClient().drop_role(role_name);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> get_role_names() throws MetaException, TException {
    return getPrimaryClient().get_role_names();
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean grant_role(
      String role_name,
      String principal_name,
      PrincipalType principal_type,
      String grantor,
      PrincipalType grantorType,
      boolean grant_option)
      throws MetaException, TException {
    return getPrimaryClient().grant_role(role_name, principal_name, principal_type, grantor, grantorType, grant_option);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean revoke_role(String role_name, String principal_name, PrincipalType principal_type)
      throws MetaException, TException {
    return getPrimaryClient().revoke_role(role_name, principal_name, principal_type);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<Role> list_roles(String principal_name, PrincipalType principal_type) throws MetaException, TException {
    return getPrimaryClient().list_roles(principal_name, principal_type);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GrantRevokeRoleResponse grant_revoke_role(GrantRevokeRoleRequest request) throws MetaException, TException {
    return getPrimaryClient().grant_revoke_role(request);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest request)
      throws MetaException, TException {
    return getPrimaryClient().get_principals_in_role(request);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(GetRoleGrantsForPrincipalRequest request)
      throws MetaException, TException {
    return getPrimaryClient().get_role_grants_for_principal(request);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String user_name, List<String> group_names)
      throws MetaException, TException {
    DatabaseMapping mapping;
    if (hiveObject.getDbName() == null) {
      mapping = databaseMappingService.primaryDatabaseMapping();
    } else {
      mapping = databaseMappingService.databaseMapping(hiveObject.getDbName());
    }
    return mapping.getClient().get_privilege_set(mapping.transformInboundHiveObjectRef(hiveObject), user_name,
            group_names);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<HiveObjectPrivilege> list_privileges(
      String principal_name,
      PrincipalType principal_type,
      HiveObjectRef hiveObject)
      throws MetaException, TException {
    DatabaseMapping mapping = databaseMappingService.databaseMapping(hiveObject.getDbName());
    List<HiveObjectPrivilege> privileges = mapping
        .getClient()
        .list_privileges(principal_name, principal_type, mapping.transformInboundHiveObjectRef(hiveObject));
    return mapping.transformOutboundHiveObjectPrivileges(privileges);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean grant_privileges(PrivilegeBag privileges) throws MetaException, TException {
    if (privileges.isSetPrivileges() && !privileges.getPrivileges().isEmpty()) {
      DatabaseMapping mapping = checkWritePermissionsForPrivileges(privileges);
      return mapping.getClient().grant_privileges(mapping.transformInboundPrivilegeBag(privileges));
    }
    return false;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean revoke_privileges(PrivilegeBag privileges) throws MetaException, TException {
    if (privileges.isSetPrivileges() && !privileges.getPrivileges().isEmpty()) {
      DatabaseMapping mapping = checkWritePermissionsForPrivileges(privileges);
      return mapping.getClient().revoke_privileges(mapping.transformInboundPrivilegeBag(privileges));
    }
    return false;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GrantRevokePrivilegeResponse grant_revoke_privileges(GrantRevokePrivilegeRequest request)
      throws MetaException, TException {
    PrivilegeBag privilegesBag = request.getPrivileges();
    if (privilegesBag.isSetPrivileges() && !privilegesBag.getPrivileges().isEmpty()) {
      DatabaseMapping mapping = checkWritePermissionsForPrivileges(privilegesBag);
      return mapping.getClient().grant_revoke_privileges(mapping.transformInboundGrantRevokePrivilegesRequest(request));
    }
    return getPrimaryClient().grant_revoke_privileges(request);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GrantRevokePrivilegeResponse refresh_privileges(HiveObjectRef hiveObjectRef, String authorizer,
                                                         GrantRevokePrivilegeRequest grantRevokePrivilegeRequest) throws MetaException, TException {
    DatabaseMapping databaseMapping = checkWritePermissions(hiveObjectRef.getDbName());
    return databaseMapping.getClient().refresh_privileges(databaseMapping.transformInboundHiveObjectRef(hiveObjectRef),
            authorizer, databaseMapping.transformInboundGrantRevokePrivilegesRequest(grantRevokePrivilegeRequest));
  }

  private DatabaseMapping checkWritePermissionsForPrivileges(PrivilegeBag privileges) throws NoSuchObjectException {
    DatabaseMapping mapping = databaseMappingService
        .databaseMapping(privileges.getPrivileges().get(0).getHiveObject().getDbName());
    for (HiveObjectPrivilege privilege : privileges.getPrivileges()) {
      HiveObjectRef obj = privilege.getHiveObject();
      mapping.checkWritePermissions(obj.getDbName());
      if (obj.getObjectType() == HiveObjectType.DATABASE) {
        mapping.checkWritePermissions(obj.getObjectName());
      }
    }
    return mapping;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> set_ugi(String user_name, List<String> group_names) throws MetaException, TException {
    List<DatabaseMapping> mappings = databaseMappingService.getAllDatabaseMappings();
    return databaseMappingService.getPanopticOperationHandler().setUgi(user_name, group_names, mappings);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public String get_delegation_token(String token_owner, String renewer_kerberos_principal_name)
      throws MetaException, TException {
    try {
      return saslServerWrapper.getDelegationTokenManager()
          .getDelegationToken(token_owner, renewer_kerberos_principal_name,
              saslServerWrapper.getIPAddress());
    } catch (IOException | InterruptedException e) {
      throw new MetaException(e.getMessage());
    }
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public long renew_delegation_token(String token_str_form) throws MetaException, TException {
    try {
      return saslServerWrapper.getDelegationTokenManager()
          .renewDelegationToken(token_str_form);
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    } catch (Exception e) {
      throw newMetaException(e);
    }
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void cancel_delegation_token(String token_str_form) throws MetaException, TException {
    try {
      saslServerWrapper.getDelegationTokenManager()
          .cancelDelegationToken(token_str_form);
    } catch (IOException e) {
      throw new MetaException(e.getMessage());
    } catch (Exception e) {
      throw newMetaException(e);
    }
  }

  private static MetaException newMetaException(Exception e) {
    if (e instanceof MetaException) {
      return (MetaException)e;
    }
    MetaException me = new MetaException(e.toString());
    me.initCause(e);
    return me;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GetOpenTxnsResponse get_open_txns() throws TException {
    return getPrimaryClient().get_open_txns();
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GetOpenTxnsInfoResponse get_open_txns_info() throws TException {
    return getPrimaryClient().get_open_txns_info();
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public OpenTxnsResponse open_txns(OpenTxnRequest rqst) throws TException {
    return getPrimaryClient().open_txns(rqst);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void abort_txn(AbortTxnRequest rqst) throws NoSuchTxnException, TException {
    getPrimaryClient().abort_txn(rqst);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void commit_txn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException, TException {
    getPrimaryClient().commit_txn(rqst);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void repl_tbl_writeid_state(ReplTblWriteIdStateRequest replTblWriteIdStateRequest) throws TException {
    DatabaseMapping databaseMapping = checkWritePermissions(replTblWriteIdStateRequest.getDbName());
    databaseMapping.getClient().repl_tbl_writeid_state(databaseMapping.transformInboundReplTblWriteIdStateRequest(replTblWriteIdStateRequest));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GetValidWriteIdsResponse get_valid_write_ids(GetValidWriteIdsRequest getValidWriteIdsRequest) throws NoSuchTxnException, MetaException, TException {
    return getPrimaryClient().get_valid_write_ids(getValidWriteIdsRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public AllocateTableWriteIdsResponse allocate_table_write_ids(AllocateTableWriteIdsRequest allocateTableWriteIdsRequest) throws NoSuchTxnException, TxnAbortedException, MetaException, TException {
    DatabaseMapping databaseMapping = checkWritePermissions(allocateTableWriteIdsRequest.getDbName());
    return databaseMapping.getClient().allocate_table_write_ids(databaseMapping.
            transformInboundAllocateTableWriteIdsRequest(allocateTableWriteIdsRequest));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public LockResponse lock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException, TException {
    DatabaseMapping mapping = databaseMappingService.primaryDatabaseMapping();
    List<LockComponent> components = rqst.getComponent();
    for (LockComponent component : components) {
      checkWritePermissionsAndCheckTableAllowed(component.getDbname(), component.getTablename());
    }
    return mapping.getClient().lock(mapping.transformInboundLockRequest(rqst));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public LockResponse check_lock(CheckLockRequest rqst)
      throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
    return getPrimaryClient().check_lock(rqst);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void unlock(UnlockRequest rqst) throws NoSuchLockException, TxnOpenException, TException {
    getPrimaryClient().unlock(rqst);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public ShowLocksResponse show_locks(ShowLocksRequest rqst) throws TException {
    return getPrimaryClient().show_locks(rqst);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void heartbeat(HeartbeatRequest ids)
      throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
    getPrimaryClient().heartbeat(ids);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public HeartbeatTxnRangeResponse heartbeat_txn_range(HeartbeatTxnRangeRequest txns) throws TException {
    return getPrimaryClient().heartbeat_txn_range(txns);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void compact(CompactionRequest rqst) throws TException {
    DatabaseMapping mapping = databaseMappingService.primaryDatabaseMapping();
    checkWritePermissionsAndCheckTableAllowed(rqst.getDbname(), rqst.getTablename(), mapping);
    mapping.getClient().compact(mapping.transformInboundCompactionRequest(rqst));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public ShowCompactResponse show_compact(ShowCompactRequest rqst) throws TException {
    return getPrimaryClient().show_compact(rqst);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public String getCpuProfile(int arg0) throws TException {
    return getPrimaryClient().getCpuProfile(arg0);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public String getVersion() throws TException {
    return getPrimaryClient().getVersion();
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public fb_status getStatus() {
    try {
      return getPrimaryClient().getStatus();
    } catch (TException e) {
      log.error("Cannot getStatus() from client: ", e);
      return fb_status.DEAD;
    }
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Configuration getConf() {
    return conf;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void init() throws MetaException {}

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public int getThreadId() {
    return HiveMetaStore.HMSHandler.get();
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public RawStore getMS() throws MetaException {
    return HiveMetaStore.HMSHandler.getRawStore();
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public TxnStore getTxnHandler() {
    return TxnUtils.getTxnStore(conf);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Warehouse getWh() {
    try {
      return new Warehouse(conf);
    } catch (MetaException e) {
      log.error("Error Instantiating Warehouse", e);
      return null;
    }
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Database get_database_core(String catalogName, String name) throws NoSuchObjectException, MetaException {
    return HiveMetaStore.HMSHandler.getRawStore().getDatabase(catalogName, name);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Table get_table_core(String catName, String dbName, String tableName) throws MetaException, NoSuchObjectException {
    return HiveMetaStore.HMSHandler.getRawStore().getTable(catName, dbName, tableName);
  }

  @Override //TODO
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<TransactionalMetaStoreEventListener> getTransactionalListeners() {
    try{
      return MetaStoreUtils.getMetaStoreListeners(TransactionalMetaStoreEventListener.class, conf,
              MetastoreConf.getVar(conf, MetastoreConf.ConfVars.TRANSACTIONAL_EVENT_LISTENERS));
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }

  @Override //TODO
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<MetaStoreEventListener> getListeners() {
    try{
      return MetaStoreUtils.getMetaStoreListeners(MetaStoreEventListener.class, conf,
              MetastoreConf.getVar(conf, MetastoreConf.ConfVars.EVENT_LISTENERS));
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
  }

  // Hive 2.1.0 methods
  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void abort_txns(AbortTxnsRequest rqst) throws NoSuchTxnException, TException {
    getPrimaryClient().abort_txns(rqst);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void add_dynamic_partitions(AddDynamicPartitions rqst)
      throws NoSuchTxnException, TxnAbortedException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(rqst.getDbname(), rqst.getTablename());
    mapping.getClient().add_dynamic_partitions(mapping.transformInboundAddDynamicPartitions(rqst));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void add_foreign_key(AddForeignKeyRequest req) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    databaseMapping.getClient().add_foreign_key(databaseMapping.transformInboundAddForeignKeyRequest(req));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void add_unique_constraint(AddUniqueConstraintRequest addUniqueConstraintRequest) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    databaseMapping.getClient().add_unique_constraint(databaseMapping.transformInboundAddUniqueConstraintRequest(addUniqueConstraintRequest));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void add_not_null_constraint(AddNotNullConstraintRequest addNotNullConstraintRequest) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    databaseMapping.getClient().add_not_null_constraint(databaseMapping.transformInboundAddNotNullConstraintRequest(addNotNullConstraintRequest));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void add_default_constraint(AddDefaultConstraintRequest addDefaultConstraintRequest) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    databaseMapping.getClient().add_default_constraint(databaseMapping.transformInboundAddDefaultConstraintRequest(addDefaultConstraintRequest));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void add_check_constraint(AddCheckConstraintRequest addCheckConstraintRequest) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    databaseMapping.getClient().add_check_constraint(databaseMapping.transformInboundAddCheckConstraintRequest(addCheckConstraintRequest));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public int add_master_key(String key) throws MetaException, TException {
    return getPrimaryClient().add_master_key(key);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void add_primary_key(AddPrimaryKeyRequest req) throws NoSuchObjectException, MetaException, TException {
    getPrimaryClient().add_primary_key(req);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean add_token(String token_identifier, String delegation_token) throws TException {
    return getPrimaryClient().add_token(token_identifier, delegation_token);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void alter_partitions_with_environment_context(
      String db_name,
      String tbl_name,
      List<Partition> new_parts,
      EnvironmentContext environment_context)
      throws InvalidOperationException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(db_name, tbl_name);
    for(Partition newPart : new_parts) {
      checkWritePermissionsAndCheckTableAllowed(newPart.getDbName(), newPart.getTableName(), mapping);
    }
    mapping
        .getClient()
        .alter_partitions_with_environment_context(mapping.transformInboundDatabaseName(db_name), tbl_name,
                mapping.transformInboundPartitions(new_parts), environment_context);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void alter_table_with_cascade(String dbname, String tbl_name, Table new_tbl, boolean cascade)
      throws InvalidOperationException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(dbname, tbl_name);
    checkWritePermissionsAndCheckTableAllowed(new_tbl.getDbName(), new_tbl.getTableName(), mapping);
    mapping
        .getClient()
        .alter_table_with_cascade(mapping.transformInboundDatabaseName(dbname), tbl_name,
                mapping.transformInboundTable(new_tbl), cascade);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public CacheFileMetadataResult cache_file_metadata(CacheFileMetadataRequest req) throws TException {
    DatabaseMapping mapping = databaseMappingService.primaryDatabaseMapping();
    checkWritePermissionsAndCheckTableAllowed(req.getDbName(), req.getTblName());
    return mapping.getClient().cache_file_metadata(mapping.transformInboundCacheFileMetadataRequest(req));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public String get_metastore_db_uuid() throws MetaException, TException {
    return getPrimaryClient().get_metastore_db_uuid();
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMCreateResourcePlanResponse create_resource_plan(WMCreateResourcePlanRequest wmCreateResourcePlanRequest) throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    return getPrimaryClient().create_resource_plan(wmCreateResourcePlanRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMGetResourcePlanResponse get_resource_plan(WMGetResourcePlanRequest wmGetResourcePlanRequest) throws NoSuchObjectException, MetaException, TException {
    return getPrimaryClient().get_resource_plan(wmGetResourcePlanRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMGetActiveResourcePlanResponse get_active_resource_plan(WMGetActiveResourcePlanRequest wmGetActiveResourcePlanRequest) throws MetaException, TException {
    return getPrimaryClient().get_active_resource_plan(wmGetActiveResourcePlanRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMGetAllResourcePlanResponse get_all_resource_plans(WMGetAllResourcePlanRequest wmGetAllResourcePlanRequest) throws MetaException, TException {
    return getPrimaryClient().get_all_resource_plans(wmGetAllResourcePlanRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMAlterResourcePlanResponse alter_resource_plan(WMAlterResourcePlanRequest wmAlterResourcePlanRequest) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    return getPrimaryClient().alter_resource_plan(wmAlterResourcePlanRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMValidateResourcePlanResponse validate_resource_plan(WMValidateResourcePlanRequest wmValidateResourcePlanRequest) throws NoSuchObjectException, MetaException, TException {
    return getPrimaryClient().validate_resource_plan(wmValidateResourcePlanRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMDropResourcePlanResponse drop_resource_plan(WMDropResourcePlanRequest wmDropResourcePlanRequest) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    return getPrimaryClient().drop_resource_plan(wmDropResourcePlanRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMCreateTriggerResponse create_wm_trigger(WMCreateTriggerRequest wmCreateTriggerRequest) throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
    return getPrimaryClient().create_wm_trigger(wmCreateTriggerRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMAlterTriggerResponse alter_wm_trigger(WMAlterTriggerRequest wmAlterTriggerRequest) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    return getPrimaryClient().alter_wm_trigger(wmAlterTriggerRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMDropTriggerResponse drop_wm_trigger(WMDropTriggerRequest wmDropTriggerRequest) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    return getPrimaryClient().drop_wm_trigger(wmDropTriggerRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMGetTriggersForResourePlanResponse get_triggers_for_resourceplan(WMGetTriggersForResourePlanRequest wmGetTriggersForResourePlanRequest) throws NoSuchObjectException, MetaException, TException {
    return getPrimaryClient().get_triggers_for_resourceplan(wmGetTriggersForResourePlanRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMCreatePoolResponse create_wm_pool(WMCreatePoolRequest wmCreatePoolRequest) throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
    return getPrimaryClient().create_wm_pool(wmCreatePoolRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMAlterPoolResponse alter_wm_pool(WMAlterPoolRequest wmAlterPoolRequest) throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
    return getPrimaryClient().alter_wm_pool(wmAlterPoolRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMDropPoolResponse drop_wm_pool(WMDropPoolRequest wmDropPoolRequest) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    return getPrimaryClient().drop_wm_pool(wmDropPoolRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMCreateOrUpdateMappingResponse create_or_update_wm_mapping(WMCreateOrUpdateMappingRequest wmCreateOrUpdateMappingRequest) throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
    return getPrimaryClient().create_or_update_wm_mapping(wmCreateOrUpdateMappingRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMDropMappingResponse drop_wm_mapping(WMDropMappingRequest wmDropMappingRequest) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    return getPrimaryClient().drop_wm_mapping(wmDropMappingRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public WMCreateOrDropTriggerToPoolMappingResponse create_or_drop_wm_trigger_to_pool_mapping(WMCreateOrDropTriggerToPoolMappingRequest
                                                                                                        wmCreateOrDropTriggerToPoolMappingRequest) throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
    return getPrimaryClient().create_or_drop_wm_trigger_to_pool_mapping(wmCreateOrDropTriggerToPoolMappingRequest);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void create_ischema(ISchema iSchema) throws AlreadyExistsException, NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = checkWritePermissions(iSchema.getDbName());
    checkWritePermissions(iSchema.getDbName());
    databaseMapping.getClient().create_ischema(databaseMapping.transformInboundISchema(iSchema));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void alter_ischema(AlterISchemaRequest alterISchemaRequest) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = checkWritePermissions(alterISchemaRequest.getName().getDbName());
    checkWritePermissions(alterISchemaRequest.getNewSchema().getDbName());
    databaseMapping.getClient().alter_ischema(databaseMapping.transformInboundAlterISchemaRequest(alterISchemaRequest));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public ISchema get_ischema(ISchemaName iSchemaName) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = checkWritePermissions(iSchemaName.getDbName());
    ISchema result = databaseMapping.getClient().get_ischema(databaseMapping.transformInboundISchemaName(iSchemaName));
    return databaseMapping.transformOutboundISchema(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void drop_ischema(ISchemaName iSchemaName) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    DatabaseMapping databaseMapping = checkWritePermissions(iSchemaName.getDbName());
    databaseMapping.getClient().drop_ischema(databaseMapping.transformInboundISchemaName(iSchemaName));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void add_schema_version(SchemaVersion schemaVersion) throws AlreadyExistsException, NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = checkWritePermissions(schemaVersion.getSchema().getDbName());
    databaseMapping.getClient().add_schema_version(databaseMapping.transformInboundSchemaVersion(schemaVersion));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public SchemaVersion get_schema_version(SchemaVersionDescriptor schemaVersionDescriptor) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = checkWritePermissions(schemaVersionDescriptor.getSchema().getDbName());
    SchemaVersion result = databaseMapping.getClient().get_schema_version(databaseMapping.
            transformInboundSchemaVersionDescriptor(schemaVersionDescriptor));
    return databaseMapping.transformOutboundSchemaVersion(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public SchemaVersion get_schema_latest_version(ISchemaName iSchemaName) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = checkWritePermissions(iSchemaName.getDbName());
    SchemaVersion result = databaseMapping.getClient().get_schema_latest_version(databaseMapping.
            transformInboundISchemaName(iSchemaName));
    return databaseMapping.transformOutboundSchemaVersion(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<SchemaVersion> get_schema_all_versions(ISchemaName iSchemaName) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = checkWritePermissions(iSchemaName.getDbName());
    List<SchemaVersion> result = databaseMapping.getClient().get_schema_all_versions(databaseMapping.
            transformInboundISchemaName(iSchemaName));
    return databaseMapping.transformOutboundSchemaVersions(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void drop_schema_version(SchemaVersionDescriptor schemaVersionDescriptor) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = checkWritePermissions(schemaVersionDescriptor.getSchema().getDbName());
    databaseMapping.getClient().drop_schema_version(databaseMapping.transformInboundSchemaVersionDescriptor(schemaVersionDescriptor));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public FindSchemasByColsResp get_schemas_by_cols(FindSchemasByColsRqst findSchemasByColsRqst) throws MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    FindSchemasByColsResp result = databaseMapping.getClient().get_schemas_by_cols(findSchemasByColsRqst);
    return databaseMapping.transformOutboundFindSchemasByColsResp(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void map_schema_version_to_serde(MapSchemaVersionToSerdeRequest mapSchemaVersionToSerdeRequest) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    databaseMapping.getClient().map_schema_version_to_serde(databaseMapping.
            transformInboundMapSchemaVersionToSerdeRequest(mapSchemaVersionToSerdeRequest));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void set_schema_version_state(SetSchemaVersionStateRequest setSchemaVersionStateRequest) throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    databaseMapping.getClient().set_schema_version_state(databaseMapping.
            transformInboundSetSchemaVersionStateRequest(setSchemaVersionStateRequest));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void add_serde(SerDeInfo serDeInfo) throws AlreadyExistsException, MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    databaseMapping.getClient().add_serde(serDeInfo);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public SerDeInfo get_serde(GetSerdeRequest getSerdeRequest) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    SerDeInfo result = databaseMapping.getClient().get_serde(getSerdeRequest);
    return result;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public LockResponse get_lock_materialization_rebuild(String dbName, String tableName, long txnId) throws TException {
    DatabaseMapping databaseMapping = databaseMappingService.databaseMapping(dbName);
    LockResponse result = databaseMapping.getClient().get_lock_materialization_rebuild(
            databaseMapping.transformInboundDatabaseName(dbName), tableName, txnId);
    return result;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean heartbeat_lock_materialization_rebuild(String dbName, String tableName, long txnId) throws TException {
    DatabaseMapping databaseMapping = databaseMappingService.databaseMapping(dbName);
    boolean result = databaseMapping.getClient().heartbeat_lock_materialization_rebuild(
            databaseMapping.transformInboundDatabaseName(dbName), tableName, txnId);
    return result;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void add_runtime_stats(RuntimeStat runtimeStat) throws MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    databaseMapping.getClient().add_runtime_stats(runtimeStat);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<RuntimeStat> get_runtime_stats(GetRuntimeStatsRequest getRuntimeStatsRequest) throws MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    List<RuntimeStat> result = databaseMapping.getClient().get_runtime_stats(getRuntimeStatsRequest);
    return result;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public ClearFileMetadataResult clear_file_metadata(ClearFileMetadataRequest req) throws TException {
    return getPrimaryClient().clear_file_metadata(req);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void drop_constraint(DropConstraintRequest req) throws NoSuchObjectException, MetaException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(req.getDbname(), req.getTablename());
    mapping.getClient().drop_constraint(mapping.transformInboundDropConstraintRequest(req));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<Partition> exchange_partitions(
      Map<String, String> partitionSpecs,
      String source_db,
      String source_table_name,
      String dest_db,
      String dest_table_name)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    DatabaseMapping mapping = checkWritePermissionsAndCheckTableAllowed(source_db, source_table_name);
    checkWritePermissionsAndCheckTableAllowed(dest_db, dest_table_name, mapping);
    List<Partition> result = mapping
        .getClient()
        .exchange_partitions(partitionSpecs, mapping.transformInboundDatabaseName(source_db), source_table_name,
            mapping.transformInboundDatabaseName(dest_db), dest_table_name);
    return mapping.transformOutboundPartitions(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public FireEventResponse fire_listener_event(FireEventRequest rqst) throws TException {
    DatabaseMapping mapping = databaseMappingService.databaseMapping(rqst.getDbName());
    return mapping.getClient().fire_listener_event(mapping.transformInboundFireEventRequest(rqst));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void flushCache() throws TException {
    getPrimaryClient().flushCache();
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public CmRecycleResponse cm_recycle(CmRecycleRequest cmRecycleRequest) throws MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.primaryDatabaseMapping();
    CmRecycleResponse result = databaseMapping.getClient().cm_recycle(cmRecycleRequest);
    return result;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GetAllFunctionsResponse get_all_functions() throws TException {
    if(waggleDanceConfiguration.isQueryFunctionsAcrossAllMetastores()) {
      return databaseMappingService
          .getPanopticOperationHandler()
          .getAllFunctions(databaseMappingService.getAvailableDatabaseMappings());
    } else {
      return getPrimaryClient().get_all_functions();
    }
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> get_all_token_identifiers() throws TException {
    return getPrimaryClient().get_all_token_identifiers();
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public CurrentNotificationEventId get_current_notificationEventId() throws TException {
    return getPrimaryClient().get_current_notificationEventId();
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public NotificationEventsCountResponse get_notification_events_count(NotificationEventsCountRequest notificationEventsCountRequest) throws TException {
    DatabaseMapping databaseMapping = getDbMappingAndCheckTableAllowed(notificationEventsCountRequest.getDbName(), notificationEventsCountRequest.getCatName());
    NotificationEventsCountResponse result = databaseMapping.getClient().get_notification_events_count(
            databaseMapping.transformInboundNotificationEventsCountRequest(notificationEventsCountRequest));
    return result;
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<FieldSchema> get_fields_with_environment_context(
      String db_name,
      String table_name,
      EnvironmentContext environment_context)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, table_name);
    return mapping
        .getClient()
        .get_fields_with_environment_context(mapping.transformInboundDatabaseName(db_name), table_name,
            environment_context);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GetFileMetadataResult get_file_metadata(GetFileMetadataRequest req) throws TException {
    return getPrimaryClient().get_file_metadata(req);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GetFileMetadataByExprResult get_file_metadata_by_expr(GetFileMetadataByExprRequest req) throws TException {
    return getPrimaryClient().get_file_metadata_by_expr(req);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public ForeignKeysResponse get_foreign_keys(ForeignKeysRequest request)
      throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(request.getForeign_db_name(), request.getForeign_tbl_name());
    return mapping
        .transformOutboundForeignKeysResponse(
            mapping.getClient().get_foreign_keys(mapping.transformInboundForeignKeysRequest(request)));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public UniqueConstraintsResponse get_unique_constraints(UniqueConstraintsRequest uniqueConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping databaseMapping = getDbMappingAndCheckTableAllowed(uniqueConstraintsRequest.getDb_name(), uniqueConstraintsRequest.getTbl_name());
    UniqueConstraintsResponse result = databaseMapping.getClient().get_unique_constraints(
            databaseMapping.transformInboundUniqueConstraintsRequest(uniqueConstraintsRequest));
    return databaseMapping.transformOutboundUniqueConstraintsResponse(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public NotNullConstraintsResponse get_not_null_constraints(NotNullConstraintsRequest notNullConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping databaseMapping = getDbMappingAndCheckTableAllowed(notNullConstraintsRequest.getDb_name(), notNullConstraintsRequest.getTbl_name());
    NotNullConstraintsResponse result = databaseMapping.getClient().get_not_null_constraints(
            databaseMapping.transformInboundNotNullConstraintsRequest(notNullConstraintsRequest));
    return databaseMapping.transformOutboundNotNullConstraintsResponse(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public DefaultConstraintsResponse get_default_constraints(DefaultConstraintsRequest defaultConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping databaseMapping = getDbMappingAndCheckTableAllowed(defaultConstraintsRequest.getDb_name(), defaultConstraintsRequest.getTbl_name());
    DefaultConstraintsResponse result = databaseMapping.getClient().get_default_constraints(
            databaseMapping.transformInboundDefaultConstraintsRequest(defaultConstraintsRequest));
    return databaseMapping.transformOutboundDefaultConstraintsResponse(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public CheckConstraintsResponse get_check_constraints(CheckConstraintsRequest checkConstraintsRequest) throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping databaseMapping = getDbMappingAndCheckTableAllowed(checkConstraintsRequest.getDb_name(), checkConstraintsRequest.getTbl_name());
    CheckConstraintsResponse result = databaseMapping.getClient().get_check_constraints(
            databaseMapping.transformInboundCheckConstraintsRequest(checkConstraintsRequest));
    return databaseMapping.transformOutboundCheckConstraintsResponse(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> get_master_keys() throws TException {
    return getPrimaryClient().get_master_keys();
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public NotificationEventResponse get_next_notification(NotificationEventRequest rqst) throws TException {
    return getPrimaryClient().get_next_notification(rqst);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public int get_num_partitions_by_filter(String db_name, String tbl_name, String filter)
      throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, tbl_name);
    return mapping
        .getClient()
        .get_num_partitions_by_filter(mapping.transformInboundDatabaseName(db_name), tbl_name, filter);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public PrimaryKeysResponse get_primary_keys(PrimaryKeysRequest request)
      throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(request.getDb_name(), request.getTbl_name());
    return mapping
        .transformOutboundPrimaryKeysResponse(
            mapping.getClient().get_primary_keys(mapping.transformInboundPrimaryKeysRequest(request)));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<FieldSchema> get_schema_with_environment_context(
      String db_name,
      String table_name,
      EnvironmentContext environment_context)
      throws MetaException, UnknownTableException, UnknownDBException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(db_name, table_name);
    return mapping
        .getClient()
        .get_schema_with_environment_context(mapping.transformInboundDatabaseName(db_name), table_name,
            environment_context);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<TableMeta> get_table_meta(String db_patterns, String tbl_patterns, List<String> tbl_types)
      throws MetaException {
    return databaseMappingService.getPanopticOperationHandler()
        .getTableMeta(db_patterns, tbl_patterns, tbl_types);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public String get_token(String token_identifier) throws TException {
    return getPrimaryClient().get_token(token_identifier);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public PutFileMetadataResult put_file_metadata(PutFileMetadataRequest req) throws TException {
    return getPrimaryClient().put_file_metadata(req);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean remove_master_key(int key_seq) throws TException {
    return getPrimaryClient().remove_master_key(key_seq);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public boolean remove_token(String token_identifier) throws TException {
    return getPrimaryClient().remove_token(token_identifier);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void update_master_key(int seq_number, String key) throws NoSuchObjectException, MetaException, TException {
    getPrimaryClient().update_master_key(seq_number, key);
  }

  // Hive 2.3.0 methods
  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public List<String> get_tables_by_type(String db_name, String pattern, String tableType)
      throws MetaException, TException {
    DatabaseMapping mapping = databaseMappingService.databaseMapping(db_name);
    List<String> resultTables = mapping.getClient().get_tables_by_type(mapping.transformInboundDatabaseName(db_name), pattern, tableType);
    List<String> result = databaseMappingService.filterTables(db_name, resultTables, mapping);
    return mapping.getMetastoreFilter().filterTableNames(null, mapping.transformInboundDatabaseName(db_name), result);
  }

  @Override
  public List<String> get_materialized_views_for_rewriting(String dbName) throws MetaException, TException {
    DatabaseMapping databaseMapping = databaseMappingService.databaseMapping(dbName);
    return databaseMapping.getClient().get_materialized_views_for_rewriting(databaseMapping.transformInboundDatabaseName(dbName));
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GetTableResult get_table_req(GetTableRequest req) throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(req.getDbName(), req.getTblName());
    GetTableResult result = mapping.getClient().get_table_req(mapping.transformInboundGetTableRequest(req));
    result.setTable(mapping.getMetastoreFilter().filterTable(result.getTable()));
    return mapping.transformOutboundGetTableResult(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public GetTablesResult get_table_objects_by_name_req(GetTablesRequest req)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    DatabaseMapping mapping = databaseMappingService.databaseMapping(req.getDbName());
    List<String> filteredTables = databaseMappingService.filterTables(req.getDbName(), req.getTblNames(), mapping);
    req.setTblNames(filteredTables);
    GetTablesResult result = mapping
        .getClient()
        .get_table_objects_by_name_req(mapping.transformInboundGetTablesRequest(req));
    result.setTables(mapping.getMetastoreFilter().filterTables(result.getTables()));
    return mapping.transformOutboundGetTablesResult(result);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public Materialization get_materialization_invalidation_info(CreationMetadata creationMetadata, String validTxnList) throws MetaException, InvalidOperationException, UnknownDBException, TException {
    DatabaseMapping databaseMapping = getDbMappingAndCheckTableAllowed(creationMetadata.getDbName(), creationMetadata.getTblName());
    return databaseMapping.getClient().get_materialization_invalidation_info(databaseMapping.
            transformInboundCreationMetadata(creationMetadata), validTxnList);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public void update_creation_metadata(String catName, String dbName, String tableName, CreationMetadata creationMetadata) throws MetaException, InvalidOperationException, UnknownDBException, TException {
    DatabaseMapping databaseMapping = getDbMappingAndCheckTableAllowed(dbName, tableName);
    databaseMapping.getClient().update_creation_metadata(catName, databaseMapping.transformInboundDatabaseName(dbName),
            tableName, creationMetadata);
  }

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public CompactionResponse compact2(CompactionRequest rqst) throws TException {
    DatabaseMapping mapping = databaseMappingService.primaryDatabaseMapping();
    checkWritePermissionsAndCheckTableAllowed(rqst.getDbname(), rqst.getTablename(), mapping);
    return mapping.getClient().compact2(mapping.transformInboundCompactionRequest(rqst));
  }

  // Hive 2.3.6 methods

  @Override
  @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
  public PartitionValuesResponse get_partition_values(PartitionValuesRequest req) throws MetaException, NoSuchObjectException, TException {
    DatabaseMapping mapping = getDbMappingAndCheckTableAllowed(req.getDbName(), req.getTblName());
    return mapping
        .getClient()
        .get_partition_values(mapping.transformInboundPartitionValuesRequest(req));
  }

}
