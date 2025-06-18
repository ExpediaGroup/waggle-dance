package com.hotels.bdp.waggledance.client.compatibility;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.AddCheckConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDefaultConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddNotNullConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddUniqueConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.AlterCatalogRequest;
import org.apache.hadoop.hive.metastore.api.AlterISchemaRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.CreateCatalogRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.DropCatalogRequest;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.GetCatalogRequest;
import org.apache.hadoop.hive.metastore.api.GetCatalogResponse;
import org.apache.hadoop.hive.metastore.api.GetCatalogsResponse;
import org.apache.hadoop.hive.metastore.api.GetRuntimeStatsRequest;
import org.apache.hadoop.hive.metastore.api.GetSerdeRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MapSchemaVersionToSerdeRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
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
import org.apache.hadoop.hive.metastore.api.SetSchemaVersionStateRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
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
import org.apache.thrift.TException;

public class HiveThriftMetaStoreIfaceCompatiblity2xx {
  // attempt to implement HMS 3 methods using HMS 2 calls

  private final ThriftHiveMetastore.Client client;

  public HiveThriftMetaStoreIfaceCompatiblity2xx(ThriftHiveMetastore.Client client) {
    this.client = client;
  }

  // TODO Any methods we cannot implement we shouldn't override so the original exception will be returned. 
  //TODO invocation handler can return existing error immediately
  public void create_catalog(CreateCatalogRequest createCatalogRequest)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public void alter_catalog(AlterCatalogRequest alterCatalogRequest)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public GetCatalogResponse get_catalog(GetCatalogRequest getCatalogRequest)
    throws NoSuchObjectException, MetaException, TException {
    //TODO MetaStoreUtils.getDefaultCatalog(client.getConf());
    throw new TException("Method not implemented in compatibility layer");

  }

  public GetCatalogsResponse get_catalogs() throws MetaException, TException {
    //TODO MetaStoreUtils.getDefaultCatalog(client.getConf());
    throw new TException("Method not implemented in compatibility layer");
  }

  public void drop_catalog(DropCatalogRequest dropCatalogRequest)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {}

  public void create_table_with_constraints(
      Table tbl,
      List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys,
      List<SQLUniqueConstraint> uniqueConstraints,
      List<SQLNotNullConstraint> notNullConstraints,
      List<SQLDefaultConstraint> defaultConstraints,
      List<SQLCheckConstraint> checkConstraints)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    client.create_table(tbl);
  }

  public void truncate_table(String dbName, String tableName, List<String> partNames) throws MetaException, TException {
    if (partNames == null || partNames.isEmpty()) {
      client.drop_table(dbName, tableName, false);
    } else {
      client.drop_partition(dbName, dbName, partNames, false);
    }
  }

  public GrantRevokePrivilegeResponse refresh_privileges(
      HiveObjectRef hiveObjectRef,
      String authorizer,
      GrantRevokePrivilegeRequest grantRevokePrivilegeRequest)
    throws MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public void repl_tbl_writeid_state(ReplTblWriteIdStateRequest replTblWriteIdStateRequest) throws TException {
  }

  public GetValidWriteIdsResponse get_valid_write_ids(GetValidWriteIdsRequest getValidWriteIdsRequest)
    throws NoSuchTxnException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public AllocateTableWriteIdsResponse allocate_table_write_ids(
      AllocateTableWriteIdsRequest allocateTableWriteIdsRequest)
    throws NoSuchTxnException, TxnAbortedException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public void add_unique_constraint(AddUniqueConstraintRequest addUniqueConstraintRequest)
    throws NoSuchObjectException, MetaException, TException {
    
  }

  public void add_not_null_constraint(AddNotNullConstraintRequest addNotNullConstraintRequest)
    throws NoSuchObjectException, MetaException, TException {
  }

  public void add_default_constraint(AddDefaultConstraintRequest addDefaultConstraintRequest)
    throws NoSuchObjectException, MetaException, TException {
  }

  public void add_check_constraint(AddCheckConstraintRequest addCheckConstraintRequest)
    throws NoSuchObjectException, MetaException, TException {
  }

  public String get_metastore_db_uuid() throws MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMCreateResourcePlanResponse create_resource_plan(WMCreateResourcePlanRequest wmCreateResourcePlanRequest)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMGetResourcePlanResponse get_resource_plan(WMGetResourcePlanRequest wmGetResourcePlanRequest)
    throws NoSuchObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMGetActiveResourcePlanResponse get_active_resource_plan(
      WMGetActiveResourcePlanRequest wmGetActiveResourcePlanRequest)
    throws MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMGetAllResourcePlanResponse get_all_resource_plans(WMGetAllResourcePlanRequest wmGetAllResourcePlanRequest)
    throws MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMAlterResourcePlanResponse alter_resource_plan(WMAlterResourcePlanRequest wmAlterResourcePlanRequest)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMValidateResourcePlanResponse validate_resource_plan(
      WMValidateResourcePlanRequest wmValidateResourcePlanRequest)
    throws NoSuchObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMDropResourcePlanResponse drop_resource_plan(WMDropResourcePlanRequest wmDropResourcePlanRequest)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMCreateTriggerResponse create_wm_trigger(WMCreateTriggerRequest wmCreateTriggerRequest)
    throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMAlterTriggerResponse alter_wm_trigger(WMAlterTriggerRequest wmAlterTriggerRequest)
    throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMDropTriggerResponse drop_wm_trigger(WMDropTriggerRequest wmDropTriggerRequest)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMGetTriggersForResourePlanResponse get_triggers_for_resourceplan(
      WMGetTriggersForResourePlanRequest wmGetTriggersForResourePlanRequest)
    throws NoSuchObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMCreatePoolResponse create_wm_pool(WMCreatePoolRequest wmCreatePoolRequest)
    throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMAlterPoolResponse alter_wm_pool(WMAlterPoolRequest wmAlterPoolRequest)
    throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMDropPoolResponse drop_wm_pool(WMDropPoolRequest wmDropPoolRequest)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMCreateOrUpdateMappingResponse create_or_update_wm_mapping(
      WMCreateOrUpdateMappingRequest wmCreateOrUpdateMappingRequest)
    throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMDropMappingResponse drop_wm_mapping(WMDropMappingRequest wmDropMappingRequest)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public WMCreateOrDropTriggerToPoolMappingResponse create_or_drop_wm_trigger_to_pool_mapping(
      WMCreateOrDropTriggerToPoolMappingRequest wmCreateOrDropTriggerToPoolMappingRequest)
    throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public void create_ischema(ISchema iSchema)
    throws AlreadyExistsException, NoSuchObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public void alter_ischema(AlterISchemaRequest alterISchemaRequest)
    throws NoSuchObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public ISchema get_ischema(ISchemaName iSchemaName) throws NoSuchObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public void drop_ischema(ISchemaName iSchemaName)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public void add_schema_version(SchemaVersion schemaVersion)
    throws AlreadyExistsException, NoSuchObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public SchemaVersion get_schema_version(SchemaVersionDescriptor schemaVersionDescriptor)
    throws NoSuchObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public SchemaVersion get_schema_latest_version(ISchemaName iSchemaName)
    throws NoSuchObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public List<SchemaVersion> get_schema_all_versions(ISchemaName iSchemaName)
    throws NoSuchObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public void drop_schema_version(SchemaVersionDescriptor schemaVersionDescriptor)
    throws NoSuchObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public FindSchemasByColsResp get_schemas_by_cols(FindSchemasByColsRqst findSchemasByColsRqst)
    throws MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public void map_schema_version_to_serde(MapSchemaVersionToSerdeRequest mapSchemaVersionToSerdeRequest)
    throws NoSuchObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public void set_schema_version_state(SetSchemaVersionStateRequest setSchemaVersionStateRequest)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public void add_serde(SerDeInfo serDeInfo) throws AlreadyExistsException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public SerDeInfo get_serde(GetSerdeRequest getSerdeRequest) throws NoSuchObjectException, MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public LockResponse get_lock_materialization_rebuild(String dbName, String tableName, long txnId) throws TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public boolean heartbeat_lock_materialization_rebuild(String dbName, String tableName, long txnId) throws TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public void add_runtime_stats(RuntimeStat runtimeStat) throws MetaException, TException {
  }

  public List<RuntimeStat> get_runtime_stats(GetRuntimeStatsRequest getRuntimeStatsRequest)
    throws MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public ClearFileMetadataResult clear_file_metadata(ClearFileMetadataRequest req) throws TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public CmRecycleResponse cm_recycle(CmRecycleRequest cmRecycleRequest) throws MetaException, TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public NotificationEventsCountResponse get_notification_events_count(
      NotificationEventsCountRequest notificationEventsCountRequest)
    throws TException {
    throw new TException("Method not implemented in compatibility layer");
  }

  public UniqueConstraintsResponse get_unique_constraints(UniqueConstraintsRequest uniqueConstraintsRequest)
    throws MetaException, NoSuchObjectException, TException {
    return new UniqueConstraintsResponse(Collections.emptyList());
  }

  public NotNullConstraintsResponse get_not_null_constraints(NotNullConstraintsRequest notNullConstraintsRequest)
    throws MetaException, NoSuchObjectException, TException {
    return new NotNullConstraintsResponse(Collections.emptyList());
  }

  public DefaultConstraintsResponse get_default_constraints(DefaultConstraintsRequest defaultConstraintsRequest)
    throws MetaException, NoSuchObjectException, TException {
    return new DefaultConstraintsResponse(Collections.emptyList());
  }

  public CheckConstraintsResponse get_check_constraints(CheckConstraintsRequest checkConstraintsRequest)
    throws MetaException, NoSuchObjectException, TException {
    return new CheckConstraintsResponse(Collections.emptyList());
  }

}
