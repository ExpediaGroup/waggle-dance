/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.Warehouse;
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
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.CreateCatalogRequest;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Database;
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
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetSchemaVersionStateRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
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
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.jcabi.aspects.Loggable;

import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.impl.NotifyingFederationService;
import com.hotels.bdp.waggledance.metrics.Monitored;

@Monitored
@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class FederatedHMSHandlerHive3
        extends FederatedHMSHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(FederatedHMSHandlerHive3.class);

    private AtomicInteger id = new AtomicInteger();

    FederatedHMSHandlerHive3(MappingEventListener databaseMappingService, NotifyingFederationService notifyingFederationService)
    {
        super(databaseMappingService, notifyingFederationService);
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public int getThreadId()
    {
        return id.incrementAndGet();
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public RawStore getMS()
            throws MetaException
    {
        throw new IllegalStateException("Called internally to metastore only so cannot be called by the proxy");
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public TxnStore getTxnHandler()
    {
        throw new IllegalStateException("Called internally to metastore only so cannot be called by the proxy");
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public Warehouse getWh()
    {
        throw new IllegalStateException("Called internally to metastore only so cannot be called by the proxy");
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public Database get_database_core(String catName, String name)
            throws NoSuchObjectException, MetaException
    {
        throw new IllegalStateException("Called internally to metastore only so cannot be called by the proxy");
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public Table get_table_core(String catName, String dbname, String name)
            throws MetaException, NoSuchObjectException
    {
        throw new IllegalStateException("Called internally to metastore only so cannot be called by the proxy");
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public List<TransactionalMetaStoreEventListener> getTransactionalListeners()
    {
        throw new IllegalStateException("Called internally to metastore only so cannot be called by the proxy");
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public List<MetaStoreEventListener> getListeners()
    {
        throw new IllegalStateException("Called internally to metastore only so cannot be called by the proxy");
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void create_catalog(CreateCatalogRequest createCatalogRequest)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException
    {
        getPrimaryClient().create_catalog(createCatalogRequest);
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void alter_catalog(AlterCatalogRequest alterCatalogRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {
        getPrimaryClient().alter_catalog(alterCatalogRequest);
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public GetCatalogResponse get_catalog(GetCatalogRequest getCatalogRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        return getPrimaryClient().get_catalog(getCatalogRequest);
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public GetCatalogsResponse get_catalogs()
            throws MetaException, TException
    {
        return getPrimaryClient().get_catalogs();
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void drop_catalog(DropCatalogRequest dropCatalogRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {
        getPrimaryClient().drop_catalog(dropCatalogRequest);
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void add_unique_constraint(AddUniqueConstraintRequest addUniqueConstraintRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        getPrimaryClient().add_unique_constraint(addUniqueConstraintRequest);
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void add_not_null_constraint(AddNotNullConstraintRequest addNotNullConstraintRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        getPrimaryClient().add_not_null_constraint(addNotNullConstraintRequest);
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void add_default_constraint(AddDefaultConstraintRequest addDefaultConstraintRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        getPrimaryClient().add_default_constraint(addDefaultConstraintRequest);
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void add_check_constraint(AddCheckConstraintRequest addCheckConstraintRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        getPrimaryClient().add_check_constraint(addCheckConstraintRequest);
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void truncate_table(String dbName, String tableName, List<String> partNames)
            throws MetaException, TException
    {
        getPrimaryClient().truncate_table(dbName, tableName, partNames);
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public List<String> get_materialized_views_for_rewriting(String view)
            throws MetaException, TException
    {
        return getPrimaryClient().get_materialized_views_for_rewriting(view);
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public Materialization get_materialization_invalidation_info(CreationMetadata creationMetadata, String s)
            throws MetaException, InvalidOperationException, UnknownDBException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void update_creation_metadata(String s, String s1, String s2, CreationMetadata creationMetadata)
            throws MetaException, InvalidOperationException, UnknownDBException, TException
    {

    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public UniqueConstraintsResponse get_unique_constraints(UniqueConstraintsRequest uniqueConstraintsRequest)
            throws MetaException, NoSuchObjectException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public NotNullConstraintsResponse get_not_null_constraints(NotNullConstraintsRequest notNullConstraintsRequest)
            throws MetaException, NoSuchObjectException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public DefaultConstraintsResponse get_default_constraints(DefaultConstraintsRequest defaultConstraintsRequest)
            throws MetaException, NoSuchObjectException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public CheckConstraintsResponse get_check_constraints(CheckConstraintsRequest checkConstraintsRequest)
            throws MetaException, NoSuchObjectException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public GrantRevokePrivilegeResponse refresh_privileges(HiveObjectRef hiveObjectRef, String s, GrantRevokePrivilegeRequest grantRevokePrivilegeRequest)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void repl_tbl_writeid_state(ReplTblWriteIdStateRequest replTblWriteIdStateRequest)
            throws TException
    {

    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public GetValidWriteIdsResponse get_valid_write_ids(GetValidWriteIdsRequest getValidWriteIdsRequest)
            throws NoSuchTxnException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public AllocateTableWriteIdsResponse allocate_table_write_ids(AllocateTableWriteIdsRequest allocateTableWriteIdsRequest)
            throws NoSuchTxnException, TxnAbortedException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public NotificationEventsCountResponse get_notification_events_count(NotificationEventsCountRequest notificationEventsCountRequest)
            throws TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public CmRecycleResponse cm_recycle(CmRecycleRequest cmRecycleRequest)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public String get_metastore_db_uuid()
            throws MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMCreateResourcePlanResponse create_resource_plan(WMCreateResourcePlanRequest wmCreateResourcePlanRequest)
            throws AlreadyExistsException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMGetResourcePlanResponse get_resource_plan(WMGetResourcePlanRequest wmGetResourcePlanRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMGetActiveResourcePlanResponse get_active_resource_plan(WMGetActiveResourcePlanRequest wmGetActiveResourcePlanRequest)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMGetAllResourcePlanResponse get_all_resource_plans(WMGetAllResourcePlanRequest wmGetAllResourcePlanRequest)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMAlterResourcePlanResponse alter_resource_plan(WMAlterResourcePlanRequest wmAlterResourcePlanRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMValidateResourcePlanResponse validate_resource_plan(WMValidateResourcePlanRequest wmValidateResourcePlanRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMDropResourcePlanResponse drop_resource_plan(WMDropResourcePlanRequest wmDropResourcePlanRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMCreateTriggerResponse create_wm_trigger(WMCreateTriggerRequest wmCreateTriggerRequest)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMAlterTriggerResponse alter_wm_trigger(WMAlterTriggerRequest wmAlterTriggerRequest)
            throws NoSuchObjectException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMDropTriggerResponse drop_wm_trigger(WMDropTriggerRequest wmDropTriggerRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMGetTriggersForResourePlanResponse get_triggers_for_resourceplan(WMGetTriggersForResourePlanRequest wmGetTriggersForResourePlanRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMCreatePoolResponse create_wm_pool(WMCreatePoolRequest wmCreatePoolRequest)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMAlterPoolResponse alter_wm_pool(WMAlterPoolRequest wmAlterPoolRequest)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMDropPoolResponse drop_wm_pool(WMDropPoolRequest wmDropPoolRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMCreateOrUpdateMappingResponse create_or_update_wm_mapping(WMCreateOrUpdateMappingRequest wmCreateOrUpdateMappingRequest)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMDropMappingResponse drop_wm_mapping(WMDropMappingRequest wmDropMappingRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public WMCreateOrDropTriggerToPoolMappingResponse create_or_drop_wm_trigger_to_pool_mapping(WMCreateOrDropTriggerToPoolMappingRequest wmCreateOrDropTriggerToPoolMappingRequest)
            throws AlreadyExistsException, NoSuchObjectException, InvalidObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void create_ischema(ISchema iSchema)
            throws AlreadyExistsException, NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void alter_ischema(AlterISchemaRequest alterISchemaRequest)
            throws NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public ISchema get_ischema(ISchemaName iSchemaName)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void drop_ischema(ISchemaName iSchemaName)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {

    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void add_schema_version(SchemaVersion schemaVersion)
            throws AlreadyExistsException, NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public SchemaVersion get_schema_version(SchemaVersionDescriptor schemaVersionDescriptor)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public SchemaVersion get_schema_latest_version(ISchemaName iSchemaName)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public List<SchemaVersion> get_schema_all_versions(ISchemaName iSchemaName)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void drop_schema_version(SchemaVersionDescriptor schemaVersionDescriptor)
            throws NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public FindSchemasByColsResp get_schemas_by_cols(FindSchemasByColsRqst findSchemasByColsRqst)
            throws MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void map_schema_version_to_serde(MapSchemaVersionToSerdeRequest mapSchemaVersionToSerdeRequest)
            throws NoSuchObjectException, MetaException, TException
    {

    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void set_schema_version_state(SetSchemaVersionStateRequest setSchemaVersionStateRequest)
            throws NoSuchObjectException, InvalidOperationException, MetaException, TException
    {

    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void add_serde(SerDeInfo serDeInfo)
            throws AlreadyExistsException, MetaException, TException
    {

    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public SerDeInfo get_serde(GetSerdeRequest getSerdeRequest)
            throws NoSuchObjectException, MetaException, TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public LockResponse get_lock_materialization_rebuild(String s, String s1, long l)
            throws TException
    {
        return null;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public boolean heartbeat_lock_materialization_rebuild(String s, String s1, long l)
            throws TException
    {
        return false;
    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public void add_runtime_stats(RuntimeStat runtimeStat)
            throws MetaException, TException
    {

    }

    @Override
    @Loggable(value = Loggable.DEBUG, skipResult = true, name = INVOCATION_LOG_NAME)
    public List<RuntimeStat> get_runtime_stats(GetRuntimeStatsRequest getRuntimeStatsRequest)
            throws MetaException, TException
    {
        return null;
    }
}
