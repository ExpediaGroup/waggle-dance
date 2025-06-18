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
package com.hotels.bdp.waggledance.client.compatibility;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.AddCheckConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDefaultConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddNotNullConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddUniqueConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;

import lombok.AllArgsConstructor;

/**
 * This class is a best effort attempt designed to mostly not break calls made to WD when federating different versions of the Hive Metastores.
 * It does not implement all methods and it can't magically implement newer Hive features when calling a legacy Hive Metastore.
 * 
 */
@AllArgsConstructor
public class HMSCompatiblityImpl implements HiveThriftMetaStoreIfaceCompatiblity {

  private final ThriftHiveMetastore.Client client;

  // Hive 2 Methods
  // Implemented using Hive 1.x.x calls.

  /*
   * (non-Javadoc)
   * @see
   * com.hotels.bdp.waggledance.client.compatibility.HiveThriftMetaStoreIfaceCompatibility#get_table_req(org.apache.
   * hadoop.hive.metastore.api.GetTableRequest) This method is missing in Hive 1.x.x, this implementation is an attempt
   * to implement it using the Hive 1.x.x methods only.
   */
  @Override
  public GetTableResult get_table_req(GetTableRequest req) throws MetaException, NoSuchObjectException, TException {
    Table table = client.get_table(req.getDbName(), req.getTblName());
    return new GetTableResult(table);
  }

  /*
   * (non-Javadoc)
   * @see
   * com.hotels.bdp.waggledance.client.compatibility.HiveThriftMetaStoreIfaceCompatibility#get_table_objects_by_name_req
   * (org.apache. hadoop.hive.metastore.api.GetTablesRequest) This method is missing in Hive 1.x.x, this implementation
   * is an attempt to implement it using the Hive 1.x.x methods only.
   */
  @Override
  public GetTablesResult get_table_objects_by_name_req(GetTablesRequest req)
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    List<Table> tables = client.get_table_objects_by_name(req.getDbName(), req.getTblNames());
    return new GetTablesResult(tables);
  }

  /*
   * (non-Javadoc)
   * @see
   * com.hotels.bdp.waggledance.client.compatibility.HiveThriftMetaStoreIfaceCompatibility#get_primary_keys(org.apache.
   * hadoop.hive.metastore.api.PrimaryKeysRequest)
   */
  @Override
  public PrimaryKeysResponse get_primary_keys(PrimaryKeysRequest request)
    throws MetaException, NoSuchObjectException, TException {
    // making sure the table exists
    client.get_table(request.getDb_name(), request.getTbl_name());
    // get_primary_keys is not supported in hive < 2.1 so just returning empty list.
    return new PrimaryKeysResponse(Collections.emptyList());
  }

  /*
   * @Override(non-Javadoc)
   * @see
   * com.hotels.bdp.waggledance.client.compatibility.HiveThriftMetaStoreIfaceCompatibility#get_foreign_keys(org.apache.
   * hadoop.hive.metastore.api.ForeignKeysRequest)
   */
  @Override
  public ForeignKeysResponse get_foreign_keys(ForeignKeysRequest request)
    throws MetaException, NoSuchObjectException, TException {
    // making sure the table exists
    client.get_table(request.getForeign_db_name(), request.getForeign_tbl_name());
    // get_foreign_keys is not supported in hive < 2.1 so just returning empty list.
    return new ForeignKeysResponse(Collections.emptyList());
  }

  // Hive 2 Methods end #########################

  // Hive 3 methods
  // Implemented using Hive 2.x.x calls.
  @Override
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

  @Override
  public void truncate_table(String dbName, String tableName, List<String> partNames) throws MetaException, TException {
    if (partNames == null || partNames.isEmpty()) {
      client.drop_table(dbName, tableName, false);
    } else {
      client.drop_partition(dbName, dbName, partNames, false);
    }
  }

  @Override
  public void add_unique_constraint(AddUniqueConstraintRequest addUniqueConstraintRequest)
    throws NoSuchObjectException, MetaException, TException {
    // empty don't do anything
  }

  @Override
  public void add_not_null_constraint(AddNotNullConstraintRequest addNotNullConstraintRequest)
    throws NoSuchObjectException, MetaException, TException {
    // empty don't do anything
  }

  @Override
  public void add_default_constraint(AddDefaultConstraintRequest addDefaultConstraintRequest)
    throws NoSuchObjectException, MetaException, TException {
    // empty don't do anything
  }

  @Override
  public void add_check_constraint(AddCheckConstraintRequest addCheckConstraintRequest)
    throws NoSuchObjectException, MetaException, TException {
    // empty don't do anything
  }

  @Override
  public UniqueConstraintsResponse get_unique_constraints(UniqueConstraintsRequest uniqueConstraintsRequest)
    throws MetaException, NoSuchObjectException, TException {
    return new UniqueConstraintsResponse(Collections.emptyList());
  }

  @Override
  public NotNullConstraintsResponse get_not_null_constraints(NotNullConstraintsRequest notNullConstraintsRequest)
    throws MetaException, NoSuchObjectException, TException {
    return new NotNullConstraintsResponse(Collections.emptyList());
  }

  @Override
  public DefaultConstraintsResponse get_default_constraints(DefaultConstraintsRequest defaultConstraintsRequest)
    throws MetaException, NoSuchObjectException, TException {
    return new DefaultConstraintsResponse(Collections.emptyList());
  }

  @Override
  public CheckConstraintsResponse get_check_constraints(CheckConstraintsRequest checkConstraintsRequest)
    throws MetaException, NoSuchObjectException, TException {
    return new CheckConstraintsResponse(Collections.emptyList());
  }

  /**
   * Hive 3.x.x added methods that we don't implement in the compatibility layer. Leaving them here for potential future
   * fixes.
   */

  // public void create_catalog(CreateCatalogRequest createCatalogRequest)
  // throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
  //
  // }
  //
  // public void alter_catalog(AlterCatalogRequest alterCatalogRequest)
  // throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
  //
  // }
  //
  // public GetCatalogResponse get_catalog(GetCatalogRequest getCatalogRequest)
  // throws NoSuchObjectException, MetaException, TException {
  // //Potential implementation MetaStoreUtils.getDefaultCatalog(client.getConf());
  // }
  //
  // public GetCatalogsResponse get_catalogs() throws MetaException, TException {
  // //Potential implementation??? MetaStoreUtils.getDefaultCatalog(client.getConf());
  // }
  //
  // public void drop_catalog(DropCatalogRequest dropCatalogRequest)
  // throws NoSuchObjectException, InvalidOperationException, MetaException, TException {}
  //
  //
  // public GrantRevokePrivilegeResponse refresh_privileges(
  // HiveObjectRef hiveObjectRef,
  // String authorizer,
  // GrantRevokePrivilegeRequest grantRevokePrivilegeRequest)
  // throws MetaException, TException {
  //
  // }
  //
  // public void repl_tbl_writeid_state(ReplTblWriteIdStateRequest replTblWriteIdStateRequest) throws TException {
  // }
  //
  // public GetValidWriteIdsResponse get_valid_write_ids(GetValidWriteIdsRequest getValidWriteIdsRequest)
  // throws NoSuchTxnException, MetaException, TException {
  //
  // }
  //
  // public AllocateTableWriteIdsResponse allocate_table_write_ids(
  // AllocateTableWriteIdsRequest allocateTableWriteIdsRequest)
  // throws NoSuchTxnException, TxnAbortedException, MetaException, TException {
  //
  // }
  //
  //
  // public String get_metastore_db_uuid() throws MetaException, TException {
  //
  // }
  //
  // public WMCreateResourcePlanResponse create_resource_plan(WMCreateResourcePlanRequest wmCreateResourcePlanRequest)
  // throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
  //
  // }
  //
  // public WMGetResourcePlanResponse get_resource_plan(WMGetResourcePlanRequest wmGetResourcePlanRequest)
  // throws NoSuchObjectException, MetaException, TException {
  //
  // }
  //
  // public WMGetActiveResourcePlanResponse get_active_resource_plan(
  // WMGetActiveResourcePlanRequest wmGetActiveResourcePlanRequest)
  // throws MetaException, TException {
  //
  // }
  //
  // public WMGetAllResourcePlanResponse get_all_resource_plans(WMGetAllResourcePlanRequest wmGetAllResourcePlanRequest)
  // throws MetaException, TException {
  //
  // }
  //
  // public WMAlterResourcePlanResponse alter_resource_plan(WMAlterResourcePlanRequest wmAlterResourcePlanRequest)
  // throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
  //
  // }
  //
  // public WMValidateResourcePlanResponse validate_resource_plan(
  // WMValidateResourcePlanRequest wmValidateResourcePlanRequest)
  // throws NoSuchObjectException, MetaException, TException {
  //
  // }
  //
  // public WMDropResourcePlanResponse drop_resource_plan(WMDropResourcePlanRequest wmDropResourcePlanRequest)
  // throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
  //
  // }
  //
  // public WMCreateTriggerResponse create_wm_trigger(WMCreateTriggerRequest wmCreateTriggerRequest)
  // throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
  //
  // }
  //
  // public WMAlterTriggerResponse alter_wm_trigger(WMAlterTriggerRequest wmAlterTriggerRequest)
  // throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
  //
  // }
  //
  // public WMDropTriggerResponse drop_wm_trigger(WMDropTriggerRequest wmDropTriggerRequest)
  // throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
  //
  // }
  //
  // public WMGetTriggersForResourePlanResponse get_triggers_for_resourceplan(
  // WMGetTriggersForResourePlanRequest wmGetTriggersForResourePlanRequest)
  // throws NoSuchObjectException, MetaException, TException {
  //
  // }
  //
  // public WMCreatePoolResponse create_wm_pool(WMCreatePoolRequest wmCreatePoolRequest)
  // throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
  //
  // }
  //
  // public WMAlterPoolResponse alter_wm_pool(WMAlterPoolRequest wmAlterPoolRequest)
  // throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
  //
  // }
  //
  // public WMDropPoolResponse drop_wm_pool(WMDropPoolRequest wmDropPoolRequest)
  // throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
  //
  // }
  //
  // public WMCreateOrUpdateMappingResponse create_or_update_wm_mapping(
  // WMCreateOrUpdateMappingRequest wmCreateOrUpdateMappingRequest)
  // throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
  //
  // }
  //
  // public WMDropMappingResponse drop_wm_mapping(WMDropMappingRequest wmDropMappingRequest)
  // throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
  //
  // }
  //
  // public WMCreateOrDropTriggerToPoolMappingResponse create_or_drop_wm_trigger_to_pool_mapping(
  // WMCreateOrDropTriggerToPoolMappingRequest wmCreateOrDropTriggerToPoolMappingRequest)
  // throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException {
  //
  // }
  //
  // public void create_ischema(ISchema iSchema)
  // throws AlreadyExistsException, NoSuchObjectException, MetaException, TException {
  //
  // }
  //
  // public void alter_ischema(AlterISchemaRequest alterISchemaRequest)
  // throws NoSuchObjectException, MetaException, TException {
  //
  // }
  //
  // public ISchema get_ischema(ISchemaName iSchemaName) throws NoSuchObjectException, MetaException, TException {
  //
  // }
  //
  // public void drop_ischema(ISchemaName iSchemaName)
  // throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
  //
  // }
  //
  // public void add_schema_version(SchemaVersion schemaVersion)
  // throws AlreadyExistsException, NoSuchObjectException, MetaException, TException {
  //
  // }
  //
  // public SchemaVersion get_schema_version(SchemaVersionDescriptor schemaVersionDescriptor)
  // throws NoSuchObjectException, MetaException, TException {
  //
  // }
  //
  // public SchemaVersion get_schema_latest_version(ISchemaName iSchemaName)
  // throws NoSuchObjectException, MetaException, TException {
  //
  // }
  //
  // public List<SchemaVersion> get_schema_all_versions(ISchemaName iSchemaName)
  // throws NoSuchObjectException, MetaException, TException {
  //
  // }
  //
  // public void drop_schema_version(SchemaVersionDescriptor schemaVersionDescriptor)
  // throws NoSuchObjectException, MetaException, TException {
  //
  // }
  //
  // public FindSchemasByColsResp get_schemas_by_cols(FindSchemasByColsRqst findSchemasByColsRqst)
  // throws MetaException, TException {
  //
  // }
  //
  // public void map_schema_version_to_serde(MapSchemaVersionToSerdeRequest mapSchemaVersionToSerdeRequest)
  // throws NoSuchObjectException, MetaException, TException {
  //
  // }
  //
  // public void set_schema_version_state(SetSchemaVersionStateRequest setSchemaVersionStateRequest)
  // throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
  //
  // }
  //
  // public void add_serde(SerDeInfo serDeInfo) throws AlreadyExistsException, MetaException, TException {
  //
  // }
  //
  // public SerDeInfo get_serde(GetSerdeRequest getSerdeRequest) throws NoSuchObjectException, MetaException, TException
  // {
  //
  // }
  //
  // public LockResponse get_lock_materialization_rebuild(String dbName, String tableName, long txnId) throws TException
  // {
  //
  // }
  //
  // public boolean heartbeat_lock_materialization_rebuild(String dbName, String tableName, long txnId) throws
  // TException {
  //
  // }
  //
  // public void add_runtime_stats(RuntimeStat runtimeStat) throws MetaException, TException {
  // }
  //
  // public List<RuntimeStat> get_runtime_stats(GetRuntimeStatsRequest getRuntimeStatsRequest)
  // throws MetaException, TException {
  //
  // }
  //
  // public ClearFileMetadataResult clear_file_metadata(ClearFileMetadataRequest req) throws TException {
  //
  // }
  //
  // public CmRecycleResponse cm_recycle(CmRecycleRequest cmRecycleRequest) throws MetaException, TException {
  //
  // }
  //
  // public NotificationEventsCountResponse get_notification_events_count(
  // NotificationEventsCountRequest notificationEventsCountRequest)
  // throws TException {
  //
  // }

  // Hive 3 methods end ######################################

}
