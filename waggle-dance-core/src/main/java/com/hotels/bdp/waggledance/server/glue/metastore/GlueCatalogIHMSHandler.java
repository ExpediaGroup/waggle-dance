/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.waggledance.server.glue.metastore;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AddForeignKeyRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AddPrimaryKeyRequest;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.ClearFileMetadataResult;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropConstraintRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsExpr;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
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
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
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
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
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
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.TableStatsResult;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hive.hcatalog.common.HCatUtil;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.CreateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.DeleteUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionResult;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsRequest;
import com.amazonaws.services.glue.model.GetUserDefinedFunctionsResult;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateUserDefinedFunctionRequest;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.amazonaws.services.glue.model.UserDefinedFunctionInput;
import com.facebook.fb303.fb_status;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.hotels.bdp.waggledance.mapping.service.impl.MonitoredDatabaseMappingService;
import com.hotels.bdp.waggledance.server.glue.aws.AWSGlueClientFactory;
import com.hotels.bdp.waggledance.server.glue.converter.CatalogToHiveConverter;
import com.hotels.bdp.waggledance.server.glue.converter.ConverterUtils;
import com.hotels.bdp.waggledance.server.glue.converter.GlueInputConverter;
import com.hotels.bdp.waggledance.server.glue.converter.HiveToCatalogConverter;
import com.hotels.bdp.waggledance.server.glue.converter.util.BatchDeletePartitionsHelper;
import com.hotels.bdp.waggledance.server.glue.converter.util.ExpressionHelper;
import com.hotels.bdp.waggledance.server.glue.converter.util.LoggingHelper;
import com.hotels.bdp.waggledance.server.glue.converter.util.MetastoreClientUtils;
import com.hotels.bdp.waggledance.server.glue.shims.AwsGlueHiveShims;
import com.hotels.bdp.waggledance.server.glue.shims.ShimsLoader;
import com.hotels.hcommon.hive.metastore.client.api.CloseableIHMSHandler;

public class GlueCatalogIHMSHandler implements CloseableIHMSHandler {

  //TODO: Extract to Waggle-Dance-Glue jar

  private static final Logger LOG = LoggerFactory.getLogger(GlueCatalogIHMSHandler.class);

  private static final ExecutorService BATCH_DELETE_PARTITIONS_THREAD_POOL = Executors.newFixedThreadPool(5,
      new ThreadFactoryBuilder().setNameFormat("batch-delete-partitions-%d").setDaemon(true).build());
  private final AWSGlue glueClient;
  private final Warehouse warehouse;
  private final GlueMetastoreClientDelegate glueMetastoreClientDelegate;
  private final AwsGlueHiveShims hiveShims = ShimsLoader.getHiveShims();
  private final MonitoredDatabaseMappingService monitoredService;
  private Map<String, String> currentMetaVars;
  private HiveConf conf;

  public GlueCatalogIHMSHandler(HiveConf conf, MonitoredDatabaseMappingService monitoredService) {
    this(conf, monitoredService, null);
  }

  public GlueCatalogIHMSHandler(
      HiveConf conf,
      MonitoredDatabaseMappingService monitoredService,
      HiveMetaHookLoader hook) {
    this.conf = conf;
    this.monitoredService = monitoredService;
    try {
      this.glueClient = new AWSGlueClientFactory(this.conf).newClient();
      this.warehouse = new Warehouse(this.conf);
      this.glueMetastoreClientDelegate = new GlueMetastoreClientDelegate(this.conf, this.glueClient, this.warehouse);
    } catch (MetaException e) {
      throw new RuntimeException(e);
    }
    snapshotActiveConf();
  }

  private void snapshotActiveConf() {
    this.currentMetaVars = new HashMap(HiveConf.metaVars.length);
    for (HiveConf.ConfVars oneVar : HiveConf.metaVars) {
      this.currentMetaVars.put(oneVar.varname, this.conf.get(oneVar.varname, ""));
    }
  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public void shutdown() {

  }

  @Override
  public void reinitialize() {

  }

  @Override
  public String getMetaConf(String key) throws MetaException, TException {
    return null;
  }

  @Override
  public void setMetaConf(String key, String value) throws MetaException, TException {

  }

  @Override
  public void create_database(Database database)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    this.glueMetastoreClientDelegate.createDatabase(database);
  }

  @Override
  public Database get_database(String name) throws NoSuchObjectException, MetaException, TException {
    return this.glueMetastoreClientDelegate.getDatabase(name);
  }

  @Override
  public void drop_database(String dbName, boolean deleteData, boolean cascade)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    this.glueMetastoreClientDelegate.dropDatabase(dbName, deleteData, false, cascade);
  }

  @Override
  public List<String> get_databases(String pattern) throws MetaException, TException {
    return this.glueMetastoreClientDelegate.getDatabases(pattern);
  }

  @Override
  public List<String> get_all_databases() throws MetaException, TException {
    return this.glueMetastoreClientDelegate.getDatabases(".*");
  }

  @Override
  public void alter_database(String dbname, Database db) throws MetaException, NoSuchObjectException, TException {
    this.glueMetastoreClientDelegate.alterDatabase(dbname, db);
  }

  @Override
  public List<FieldSchema> get_fields(String db_name, String table_name)
    throws MetaException, UnknownTableException, UnknownDBException, TException {
    return this.glueMetastoreClientDelegate.getFields(db_name, table_name);
  }

  @Override
  public List<FieldSchema> get_schema(String db_name, String table_name)
    throws MetaException, UnknownTableException, UnknownDBException, TException {
    return this.glueMetastoreClientDelegate.getSchema(db_name, table_name);
  }

  @Override
  public void create_table(Table tbl)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    this.glueMetastoreClientDelegate.createTable(tbl);
  }

  @Override
  public void create_table_with_environment_context(Table tbl, EnvironmentContext environment_context)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    this.glueMetastoreClientDelegate.createTable(tbl);
  }

  @Override
  public void drop_table(String dbname, String name, boolean deleteData)
    throws NoSuchObjectException, MetaException, TException {
    this.glueMetastoreClientDelegate.dropTable(dbname, name, deleteData, false, false);
  }

  @Override
  public void drop_table_with_environment_context(
      String dbname,
      String name,
      boolean deleteData,
      EnvironmentContext environment_context)
    throws NoSuchObjectException, MetaException, TException {
    this.glueMetastoreClientDelegate.dropTable(dbname, name, deleteData, false, false);
  }

  @Override
  public List<String> get_tables(String db_name, String pattern) throws MetaException, TException {
    return this.glueMetastoreClientDelegate.getTables(db_name, pattern);
  }

  @Override
  public List<String> get_all_tables(String db_name) throws MetaException, TException {
    return this.glueMetastoreClientDelegate.getTables(db_name, ".*");
  }

  @Override
  public Table get_table(String dbname, String tbl_name) throws MetaException, NoSuchObjectException, TException {
    return this.glueMetastoreClientDelegate.getTable(dbname, tbl_name);
  }

  @Override
  public List<Table> get_table_objects_by_name(String dbName, List<String> tableNames)
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    List<Table> hiveTables = Lists.newArrayList();
    for (String tableName : tableNames) {
      hiveTables.add(get_table(dbName, tableName));
    }
    return hiveTables;
  }

  @Override
  public List<String> get_table_names_by_filter(String dbname, String filter, short max_tables)
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return this.glueMetastoreClientDelegate.listTableNamesByFilter(dbname, filter, max_tables);
  }

  @Override
  public void alter_table(String dbname, String tbl_name, Table new_tbl)
    throws InvalidOperationException, MetaException, TException {
    this.alter_table_with_environment_context(dbname, tbl_name, new_tbl, null);
  }

  @Override
  public void alter_table_with_environment_context(
      String dbname,
      String tbl_name,
      Table new_tbl,
      EnvironmentContext environment_context)
    throws InvalidOperationException, MetaException, TException {
    this.glueMetastoreClientDelegate.alterTable(dbname, tbl_name, new_tbl, environment_context);
  }

  @Override
  public Partition add_partition(Partition partition)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    this.glueMetastoreClientDelegate.addPartitions(Lists.newArrayList(new Partition[] { partition }), false, true);
    return partition;
  }

  @Override
  public Partition add_partition_with_environment_context(Partition new_part, EnvironmentContext environment_context)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return this.add_partition(new_part);
  }

  @Override
  public int add_partitions(List<Partition> new_parts)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return this.glueMetastoreClientDelegate.addPartitions(new_parts, true, false).size();
  }

  @Override
  public int add_partitions_pspec(List<PartitionSpec> parts)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    int i = 0;
    for (PartitionSpec partSpec : parts) {
      PartitionSpecProxy partitionSpecProxy = PartitionSpecProxy.Factory.get(partSpec);
      this.glueMetastoreClientDelegate.addPartitionsSpecProxy(partitionSpecProxy);
      i++;
    }
    return i;
  }

  @Override
  public Partition append_partition(String db_name, String tbl_name, List<String> part_vals)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return this.glueMetastoreClientDelegate.appendPartition(db_name, tbl_name, part_vals);
  }

  @Override
  public AddPartitionsResult add_partitions_req(AddPartitionsRequest request)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    AddPartitionsResult result = new AddPartitionsResult();
    result.setPartitions(this.glueMetastoreClientDelegate.addPartitions(request.getParts(), request.isIfNotExists(),
        request.isNeedResult()));
    return result;
  }

  @Override
  public Partition append_partition_with_environment_context(
      String db_name,
      String tbl_name,
      List<String> part_vals,
      EnvironmentContext environment_context)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return this.glueMetastoreClientDelegate.appendPartition(db_name, tbl_name, part_vals);
  }

  @Override
  public Partition append_partition_by_name(String dbName, String tblName, String partitionName)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    List<String> partVals = this.glueMetastoreClientDelegate.partitionNameToVals(partitionName);
    return this.glueMetastoreClientDelegate.appendPartition(dbName, tblName, partVals);
  }

  @Override
  public Partition append_partition_by_name_with_environment_context(
      String db_name,
      String tbl_name,
      String part_name,
      EnvironmentContext environment_context)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return this.append_partition_by_name(db_name, tbl_name, part_name);
  }

  @Override
  public boolean drop_partition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData)
    throws NoSuchObjectException, MetaException, TException {
    return this.glueMetastoreClientDelegate.dropPartition(db_name, tbl_name, part_vals, true, deleteData, false);
  }

  @Override
  public boolean drop_partition_with_environment_context(
      String db_name,
      String tbl_name,
      List<String> part_vals,
      boolean deleteData,
      EnvironmentContext environment_context)
    throws NoSuchObjectException, MetaException, TException {
    return this.drop_partition(db_name, tbl_name, part_vals, deleteData);
  }

  @Override
  public boolean drop_partition_by_name(String db_name, String tbl_name, String part_name, boolean deleteData)
    throws NoSuchObjectException, MetaException, TException {
    List<String> partVals = this.glueMetastoreClientDelegate.partitionNameToVals(part_name);
    return this.drop_partition(db_name, tbl_name, partVals, deleteData);
  }

  @Override
  public boolean drop_partition_by_name_with_environment_context(
      String db_name,
      String tbl_name,
      String part_name,
      boolean deleteData,
      EnvironmentContext environment_context)
    throws NoSuchObjectException, MetaException, TException {
    return this.drop_partition_by_name(db_name, tbl_name, part_name, deleteData);
  }

  @Override
  public DropPartitionsResult drop_partitions_req(DropPartitionsRequest req)
    throws NoSuchObjectException, MetaException, TException {
    List<byte[]> exprs = new ArrayList<>();
    for (DropPartitionsExpr expr : req.getParts().getExprs()) {
      exprs.add(expr.getExpr());
    }

    List<Partition> partitions = this.dropPartitions_core(req.getDbName(), req.getTblName(), exprs, req.isDeleteData(),
        false);
    DropPartitionsResult result = new DropPartitionsResult();
    result.setPartitions(partitions);
    return result;
  }

  private List<Partition> dropPartitions_core(
      String databaseName,
      String tableName,
      List<byte[]> partExprs,
      boolean deleteData,
      boolean purgeData)
    throws TException {
    List<Partition> deleted = Lists.newArrayList();
    for (byte[] expr : partExprs) {
      String exprString = ExpressionHelper.convertHiveExpressionToCatalogExpression(expr);
      List<com.amazonaws.services.glue.model.Partition> catalogPartitionsToDelete = this.glueMetastoreClientDelegate
          .getCatalogPartitions(databaseName, tableName, exprString, -1L);
      deleted.addAll(batchDeletePartitions(databaseName, tableName, catalogPartitionsToDelete, deleteData, purgeData));
    }
    return deleted;
  }

  private List<Partition> batchDeletePartitions(
      final String dbName,
      final String tableName,
      List<com.amazonaws.services.glue.model.Partition> partitionsToDelete,
      boolean deleteData,
      boolean purgeData)
    throws TException {
    List<Partition> deleted = Lists.newArrayList();
    if (partitionsToDelete == null) {
      return deleted;
    }
    validateBatchDeletePartitionsArguments(dbName, tableName, partitionsToDelete);

    List<Future<BatchDeletePartitionsHelper>> batchDeletePartitionsFutures = Lists.newArrayList();

    int numOfPartitionsToDelete = partitionsToDelete.size();
    int j;
    for (int i = 0; i < numOfPartitionsToDelete; i += 25) {
      j = Math.min(i + 25, numOfPartitionsToDelete);
      final List<com.amazonaws.services.glue.model.Partition> partitionsOnePage = partitionsToDelete.subList(i, j);

      batchDeletePartitionsFutures.add(BATCH_DELETE_PARTITIONS_THREAD_POOL.submit(new Callable() {
        public BatchDeletePartitionsHelper call() throws Exception {
          return new BatchDeletePartitionsHelper(GlueCatalogIHMSHandler.this.glueClient, dbName, tableName,
              partitionsOnePage).deletePartitions();
        }
      }));
    }
    TException tException = null;
    for (Future<BatchDeletePartitionsHelper> future : batchDeletePartitionsFutures) {
      try {
        BatchDeletePartitionsHelper batchDeletePartitionsHelper = (BatchDeletePartitionsHelper) future.get();
        for (com.amazonaws.services.glue.model.Partition partition : batchDeletePartitionsHelper
            .getPartitionsDeleted()) {
          Partition hivePartition = CatalogToHiveConverter.convertPartition(partition);
          try {
            performDropPartitionPostProcessing(dbName, tableName, hivePartition, deleteData, purgeData);
          } catch (TException e) {
            LOG.error("Drop partition directory failed.", e);
            tException = tException == null ? e : tException;
          }
          deleted.add(hivePartition);
        }
        tException = tException == null ? batchDeletePartitionsHelper.getFirstTException() : tException;
      } catch (Exception e) {
        LOG.error("Exception thrown by BatchDeletePartitions thread pool. ", e);
      }
    }
    if (tException != null) {
      throw tException;
    }
    return deleted;
  }

  private void validateBatchDeletePartitionsArguments(
      String dbName,
      String tableName,
      List<com.amazonaws.services.glue.model.Partition> partitionsToDelete) {
    Preconditions.checkArgument(dbName != null, "Database name cannot be null");
    Preconditions.checkArgument(tableName != null, "Table name cannot be null");
    for (com.amazonaws.services.glue.model.Partition partition : partitionsToDelete) {
      Preconditions.checkArgument(dbName.equals(partition.getDatabaseName()), "Database name cannot be null");
      Preconditions.checkArgument(tableName.equals(partition.getTableName()), "Table name cannot be null");
      Preconditions.checkArgument(partition.getValues() != null, "Partition values cannot be null");
    }
  }

  private void performDropPartitionPostProcessing(
      String dbName,
      String tblName,
      Partition partition,
      boolean deleteData,
      boolean ifPurge)
    throws MetaException, NoSuchObjectException, TException {
    if ((deleteData) && (partition.getSd() != null) && (partition.getSd().getLocation() != null)) {
      Path partPath = new Path(partition.getSd().getLocation());
      Table table = get_table(dbName, tblName);
      if (MetastoreClientUtils.isExternalTable(table)) {
        return;
      }
      boolean mustPurge = isMustPurge(table, ifPurge);
      this.warehouse.deleteDir(partPath, true, mustPurge);
      try {
        List<String> values = partition.getValues();
        deleteParentRecursive(partPath.getParent(), values.size() - 1, mustPurge);
      } catch (IOException e) {
        throw new MetaException(e.getMessage());
      }
    }
  }

  private boolean isMustPurge(Table table, boolean ifPurge) {
    return (ifPurge) || ("true".equalsIgnoreCase((String) table.getParameters().get("auto.purge")));
  }

  private void deleteParentRecursive(Path parent, int depth, boolean mustPurge) throws IOException, MetaException {
    if ((depth > 0) && (parent != null) && (warehouse.isWritable(parent)) && (warehouse.isEmpty(parent))) {
      warehouse.deleteDir(parent, true, mustPurge);
      deleteParentRecursive(parent.getParent(), depth - 1, mustPurge);
    }
  }

  @Override
  public Partition get_partition(String db_name, String tbl_name, List<String> part_vals)
    throws MetaException, NoSuchObjectException, TException {
    return this.glueMetastoreClientDelegate.getPartition(db_name, tbl_name, part_vals);
  }

  @Override
  public Partition exchange_partition(
      Map<String, String> partitionSpecs,
      String source_db,
      String source_table_name,
      String dest_db,
      String dest_table_name)
    throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    return this.glueMetastoreClientDelegate.exchangePartition(partitionSpecs, source_db, source_table_name, dest_db,
        dest_table_name);
  }

  @Override
  public Partition get_partition_with_auth(
      String databaseName,
      String tableName,
      List<String> values,
      String userName,
      List<String> groupNames)
    throws MetaException, NoSuchObjectException, TException {
    Partition partition = get_partition(databaseName, tableName, values);
    Table table = get_table(databaseName, tableName);
    if ("TRUE".equalsIgnoreCase((String) table.getParameters().get("PARTITION_LEVEL_PRIVILEGE"))) {
      String partName = Warehouse.makePartName(table.getPartitionKeys(), values);
      HiveObjectRef obj = new HiveObjectRef();
      obj.setObjectType(HiveObjectType.PARTITION);
      obj.setDbName(databaseName);
      obj.setObjectName(tableName);
      obj.setPartValues(values);

      PrincipalPrivilegeSet privilegeSet = get_privilege_set(obj, userName, groupNames);
      partition.setPrivileges(privilegeSet);
    }
    return partition;
  }

  @Override
  public Partition get_partition_by_name(String db_name, String tbl_name, String part_name)
    throws MetaException, NoSuchObjectException, TException {
    return this.glueMetastoreClientDelegate.getPartition(db_name, tbl_name, part_name);
  }

  @Override
  public List<Partition> get_partitions(String db_name, String tbl_name, short max_parts)
    throws NoSuchObjectException, MetaException, TException {
    return this.listPartitions(db_name, tbl_name, max_parts);
  }

  private List<Partition> listPartitions(String dbName, String tblName, short max)
    throws NoSuchObjectException, MetaException, TException {
    return listPartitions(dbName, tblName, null, max);
  }

  private List<Partition> listPartitions(String databaseName, String tableName, List<String> values, short max)
    throws NoSuchObjectException, MetaException, TException {
    String expression = null;
    if (values != null) {
      Table table = get_table(databaseName, tableName);
      expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, values);
    }
    return this.glueMetastoreClientDelegate.getPartitions(databaseName, tableName, expression, max);
  }

  @Override
  public List<Partition> get_partitions_with_auth(
      String db_name,
      String tbl_name,
      short max_parts,
      String user_name,
      List<String> group_names)
    throws NoSuchObjectException, MetaException, TException {
    return listPartitionsWithAuthInfo(db_name, tbl_name, max_parts, user_name, group_names);
  }

  private List<Partition> listPartitionsWithAuthInfo(
      String database,
      String table,
      short maxParts,
      String user,
      List<String> groups)
    throws MetaException, TException, NoSuchObjectException {
    List<Partition> partitions = listPartitions(database, table, maxParts);
    for (Partition p : partitions) {
      HiveObjectRef obj = new HiveObjectRef();
      obj.setObjectType(HiveObjectType.PARTITION);
      obj.setDbName(database);
      obj.setObjectName(table);
      obj.setPartValues(p.getValues());
      PrincipalPrivilegeSet set = get_privilege_set(obj, user, groups);
      p.setPrivileges(set);
    }
    return partitions;
  }

  private List<Partition> listPartitionsWithAuthInfo(
      String database,
      String table,
      List<String> partVals,
      short maxParts,
      String user,
      List<String> groups)
    throws MetaException, TException, NoSuchObjectException {
    List<Partition> partitions = listPartitions(database, table, partVals, maxParts);
    for (Partition p : partitions) {
      HiveObjectRef obj = new HiveObjectRef();
      obj.setObjectType(HiveObjectType.PARTITION);
      obj.setDbName(database);
      obj.setObjectName(table);
      obj.setPartValues(p.getValues());
      PrincipalPrivilegeSet set = new PrincipalPrivilegeSet();
      try {
        set = get_privilege_set(obj, user, groups);
      } catch (MetaException e) {
        LOG.info(String.format("No privileges found for user: %s, groups: [%s]",
            new Object[] { user, LoggingHelper.concatCollectionToStringForLogging(groups, ",") }));
      }
      p.setPrivileges(set);
    }
    return partitions;
  }

  @Override
  public List<PartitionSpec> get_partitions_pspec(String db_name, String tbl_name, int max_parts)
    throws NoSuchObjectException, MetaException, TException {
    return this.glueMetastoreClientDelegate.listPartitionSpecs(db_name, tbl_name, max_parts).toPartitionSpec();
  }

  @Override
  public List<String> get_partition_names(String db_name, String tbl_name, short max_parts)
    throws MetaException, TException {
    try {
      return listPartitionNames(db_name, tbl_name, null, max_parts);
    } catch (NoSuchObjectException e) {}
    return Collections.emptyList();
  }

  private List<String> listPartitionNames(String databaseName, String tableName, List<String> values, short max)
    throws TException {
    String expression = null;
    Table table = get_table(databaseName, tableName);
    if (values != null) {
      expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, values);
    }

    List<String> names = Lists.newArrayList();
    List<Partition> partitions = getPartitions(databaseName, tableName, expression, max);
    for (Partition p : partitions) {
      names.add(Warehouse.makePartName(table.getPartitionKeys(), p.getValues()));
    }
    return names;
  }

  private List<Partition> getPartitions(String databaseName, String tableName, String filter, long max)
    throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");
    List<com.amazonaws.services.glue.model.Partition> partitions = getCatalogPartitions(databaseName, tableName, filter,
        max);
    return CatalogToHiveConverter.convertPartitions(partitions);
  }

  public List<com.amazonaws.services.glue.model.Partition> getCatalogPartitions(
      String databaseName,
      String tableName,
      String expression,
      long max)
    throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");

    List<com.amazonaws.services.glue.model.Partition> partitions = Lists.newArrayList();
    if (max == 0L) {
      return partitions;
    }

    String nextToken = null;

    do {
      GetPartitionsRequest request = new GetPartitionsRequest()
          .withDatabaseName(databaseName)
          .withTableName(tableName)
          .withExpression(expression)
          .withNextToken(nextToken);
      try {
        GetPartitionsResult res = glueClient.getPartitions(request);
        List<com.amazonaws.services.glue.model.Partition> list = res.getPartitions();
        if ((partitions.size() + list.size() >= max) && (max > 0L)) {
          long remaining = max - partitions.size();
          partitions.addAll(list.subList(0, (int) remaining));
          break;
        }
        partitions.addAll(list);
        nextToken = res.getNextToken();
      } catch (AmazonServiceException e) {
        throw CatalogToHiveConverter.wrapInHiveException(e);
      } catch (Exception e) {
        String msg = "Unable to get partitions with expression: " + expression;
        LOG.error(msg, e);
        throw new MetaException(msg + e);
      }
    } while (nextToken != null);

    return partitions;
  }

  @Override
  public List<Partition> get_partitions_ps(String databaseName, String tableName, List<String> values, short max)
    throws MetaException, NoSuchObjectException, TException {
    String expression = null;
    if (values != null) {
      Table table = get_table(databaseName, tableName);
      expression = ExpressionHelper.buildExpressionFromPartialSpecification(table, values);
    }
    return this.glueMetastoreClientDelegate.getPartitions(databaseName, tableName, expression, max);
  }

  @Override
  public List<Partition> get_partitions_ps_with_auth(
      String db_name,
      String tbl_name,
      List<String> part_vals,
      short max_parts,
      String user_name,
      List<String> group_names)
    throws NoSuchObjectException, MetaException, TException {
    return listPartitionsWithAuthInfo(db_name, tbl_name, part_vals, max_parts, user_name, group_names);
  }

  @Override
  public List<String> get_partition_names_ps(String db_name, String tbl_name, List<String> part_vals, short max_parts)
    throws MetaException, NoSuchObjectException, TException {
    return this.glueMetastoreClientDelegate.listPartitionNames(db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<Partition> get_partitions_by_filter(String db_name, String tbl_name, String filter, short max_parts)
    throws MetaException, NoSuchObjectException, TException {
    return this.glueMetastoreClientDelegate.getPartitions(db_name, tbl_name, filter, max_parts);
  }

  @Override
  public List<PartitionSpec> get_part_specs_by_filter(String db_name, String tbl_name, String filter, int max_parts)
    throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException("get_part_specs_by_filter is not supported");
  }

  @Override
  public PartitionsByExprResult get_partitions_by_expr(PartitionsByExprRequest req)
    throws MetaException, NoSuchObjectException, TException {
    /*
     * String databaseName, String tableName, byte[] expr, String defaultPartitionName, short max, List<Partition>
     * result
     */
    String databaseName = req.getDbName();
    String tableName = req.getTblName();
    byte[] expr = req.getExpr();
    String defaultPartitionName = req.getDefaultPartitionName();
    short max = req.getMaxParts();
    String catalogExpression = ExpressionHelper.convertHiveExpressionToCatalogExpression(expr);
    PartitionsByExprResult partitionsByExprResult = new PartitionsByExprResult();
    partitionsByExprResult
        .setPartitions(this.glueMetastoreClientDelegate.getPartitions(databaseName, tableName, catalogExpression, max));
    return partitionsByExprResult;
  }

  @Override
  public List<Partition> get_partitions_by_names(String db_name, String tbl_name, List<String> names)
    throws MetaException, NoSuchObjectException, TException {
    return this.glueMetastoreClientDelegate.getPartitionsByNames(db_name, tbl_name, names);
  }

  @Override
  public void alter_partition(String db_name, String tbl_name, Partition new_part)
    throws InvalidOperationException, MetaException, TException {
    alter_partitions(db_name, tbl_name, Lists.newArrayList(new Partition[] { new_part }));
  }

  @Override
  public void alter_partitions(String db_name, String tbl_name, List<Partition> new_parts)
    throws InvalidOperationException, MetaException, TException {
    this.glueMetastoreClientDelegate.alterPartitions(db_name, tbl_name, new_parts);
  }

  @Override
  public void alter_partition_with_environment_context(
      String db_name,
      String tbl_name,
      Partition new_part,
      EnvironmentContext environment_context)
    throws InvalidOperationException, MetaException, TException {
    this.alter_partition(db_name, tbl_name, new_part);
  }

  @Override
  public void rename_partition(String db_name, String tbl_name, List<String> part_vals, Partition new_part)
    throws InvalidOperationException, MetaException, TException {
    this.renamePartition(db_name, tbl_name, part_vals, new_part);
  }

  public void renamePartition(String dbName, String tblName, List<String> partitionValues, Partition newPartition)
    throws InvalidOperationException, MetaException, TException {
    setDDLTime(newPartition);
    Partition oldPart = null;
    Table tbl = null;
    try {
      tbl = get_table(dbName, tblName);
      oldPart = get_partition(dbName, tblName, partitionValues);
    } catch (NoSuchObjectException e) {
      throw new InvalidOperationException(e.getMessage());
    }
    if ((newPartition.getSd() == null) || (oldPart.getSd() == null)) {
      throw new InvalidOperationException("Storage descriptor cannot be null");
    }
    if ((!Strings.isNullOrEmpty(tbl.getTableType()))
        && (tbl.getTableType().equals(TableType.EXTERNAL_TABLE.toString()))) {
      newPartition.getSd().setLocation(oldPart.getSd().getLocation());
      renamePartitionInCatalog(dbName, tblName, partitionValues, newPartition);
    } else {
      Path destPath = getDestinationPathForRename(dbName, tbl, newPartition);
      Path srcPath = new Path(oldPart.getSd().getLocation());
      FileSystem srcFs = this.warehouse.getFs(srcPath);
      FileSystem destFs = this.warehouse.getFs(destPath);

      verifyDestinationLocation(srcFs, destFs, srcPath, destPath, tbl, newPartition);
      newPartition.getSd().setLocation(destPath.toString());

      renamePartitionInCatalog(dbName, tblName, partitionValues, newPartition);
      boolean success = true;
      try {
        if (srcFs.exists(srcPath)) {
          Path destParentPath = destPath.getParent();
          if (!this.warehouse.mkdirs(destParentPath, true)) {
            throw new IOException("Unable to create path " + destParentPath);
          }
          this.warehouse.renameDir(srcPath, destPath, true);
        }
      } catch (IOException e) {
        success = false;

        throw new InvalidOperationException("Unable to access old location "
            + srcPath
            + " for partition "
            + tbl.getDbName()
            + "."
            + tbl.getTableName()
            + " "
            + partitionValues);
      } finally {
        if (!success) {
          renamePartitionInCatalog(dbName, tblName, newPartition.getValues(), oldPart);
        }
      }
    }
  }

  private void verifyDestinationLocation(
      FileSystem srcFs,
      FileSystem destFs,
      Path srcPath,
      Path destPath,
      Table tbl,
      Partition newPartition)
    throws InvalidOperationException {
    String oldPartLoc = srcPath.toString();
    String newPartLoc = destPath.toString();
    if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
      throw new InvalidOperationException("table new location "
          + destPath
          + " is on a different file system than the old location "
          + srcPath
          + ". This operation is not supported");
    }
    try {
      srcFs.exists(srcPath);
      if ((newPartLoc.compareTo(oldPartLoc) != 0) && (destFs.exists(destPath))) {
        throw new InvalidOperationException("New location for this partition "
            + tbl.getDbName()
            + "."
            + tbl.getTableName()
            + "."
            + newPartition.getValues()
            + " already exists : "
            + destPath);
      }
    } catch (IOException e) {
      throw new InvalidOperationException("Unable to access new location "
          + destPath
          + " for partition "
          + tbl.getDbName()
          + "."
          + tbl.getTableName()
          + " "
          + newPartition.getValues());
    }
  }

  private Path getDestinationPathForRename(String dbName, Table tbl, Partition newPartition)
    throws InvalidOperationException, MetaException, TException {
    try {
      Path destPath = new Path(
          this.hiveShims.getDefaultTablePath(get_database(dbName), tbl.getTableName(), this.warehouse),
          Warehouse.makePartName(tbl.getPartitionKeys(), newPartition.getValues()));
      return constructRenamedPath(destPath, new Path(newPartition.getSd().getLocation()));
    } catch (NoSuchObjectException e) {
      throw new InvalidOperationException("Unable to change partition or table. Database "
          + dbName
          + " does not exist Check metastore logs for detailed stack."
          + e.getMessage());
    }
  }

  private void setDDLTime(Partition partition) {
    if ((partition.getParameters() == null)
        || (partition.getParameters().get("transient_lastDdlTime") == null)
        || (Integer.parseInt((String) partition.getParameters().get("transient_lastDdlTime")) == 0)) {
      partition.putToParameters("transient_lastDdlTime", Long.toString(System.currentTimeMillis() / 1000L));
    }
  }

  private void renamePartitionInCatalog(
      String databaseName,
      String tableName,
      List<String> partitionValues,
      Partition newPartition)
    throws InvalidOperationException, MetaException, TException {
    try {
      this.glueClient.updatePartition(new UpdatePartitionRequest()

          .withDatabaseName(databaseName)
          .withTableName(tableName)
          .withPartitionValueList(partitionValues)
          .withPartitionInput(GlueInputConverter.convertToPartitionInput(newPartition)));
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    }
  }

  private Path constructRenamedPath(Path defaultNewPath, Path currentPath) {
    URI currentUri = currentPath.toUri();
    return new Path(currentUri.getScheme(), currentUri.getAuthority(), defaultNewPath.toUri().getPath());
  }

  @Override
  public boolean partition_name_has_valid_characters(List<String> part_vals, boolean throw_exception)
    throws MetaException, TException {
    try {
      String partitionValidationRegex = this.conf.getVar(HiveConf.ConfVars.METASTORE_PARTITION_NAME_WHITELIST_PATTERN);
      Pattern partitionValidationPattern = Strings.isNullOrEmpty(partitionValidationRegex) ? null
          : Pattern.compile(partitionValidationRegex);
      MetaStoreUtils.validatePartitionNameCharacters(part_vals, partitionValidationPattern);
    } catch (Exception e) {
      if (throw_exception) {
        if ((e instanceof MetaException)) {
          throw ((MetaException) e);
        }
        throw new MetaException(e.getMessage());
      } else {
        return false;
      }
    }
    return true;
  }

  @Override
  public String get_config_value(String name, String defaultValue) throws ConfigValSecurityException, TException {
    return conf.get(name, defaultValue);
  }

  @Override
  public List<String> partition_name_to_vals(String part_name) throws MetaException, TException {
    return this.glueMetastoreClientDelegate.partitionNameToVals(part_name);
  }

  @Override
  public Map<String, String> partition_name_to_spec(String part_name) throws MetaException, TException {
    return Warehouse.makeSpecFromName(part_name);
  }

  @Override
  public void markPartitionForEvent(
      String db_name,
      String tbl_name,
      Map<String, String> part_vals,
      PartitionEventType eventType)
    throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException, UnknownPartitionException,
    InvalidPartitionException, TException {
    this.glueMetastoreClientDelegate.markPartitionForEvent(db_name, tbl_name, part_vals, eventType);
  }

  @Override
  public boolean isPartitionMarkedForEvent(
      String db_name,
      String tbl_name,
      Map<String, String> part_vals,
      PartitionEventType eventType)
    throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException, UnknownPartitionException,
    InvalidPartitionException, TException {
    return this.glueMetastoreClientDelegate.isPartitionMarkedForEvent(db_name, tbl_name, part_vals, eventType);
  }

  @Override
  public Index add_index(Index index, Table indexTable)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    boolean dirCreated = this.glueMetastoreClientDelegate.validateNewTableAndCreateDirectory(indexTable);
    boolean indexTableCreated = false;
    String dbName = index.getDbName();
    String indexTableName = index.getIndexTableName();
    String originTableName = index.getOrigTableName();
    Path indexTablePath = new Path(indexTable.getSd().getLocation());
    com.amazonaws.services.glue.model.Table catalogIndexTableObject = HiveToCatalogConverter
        .convertIndexToTableObject(index);
    String indexTableObjectName = "index_prefix" + index.getIndexName();
    try {
      Table originTable = get_table(dbName, originTableName);
      Map<String, String> parameters = originTable.getParameters();
      if (parameters.containsKey(indexTableObjectName)) {
        throw new AlreadyExistsException("Index: " + index.getIndexName() + " already exist");
      }
      create_table(indexTable);
      indexTableCreated = true;
      originTable.getParameters().put(indexTableObjectName,
          ConverterUtils.catalogTableToString(catalogIndexTableObject));
      alter_table(dbName, originTableName, originTable);
    } catch (Exception e) {
      if (dirCreated) {
        this.warehouse.deleteDir(indexTablePath, true);
      }
      if (indexTableCreated) {
        drop_table(dbName, indexTableName, false);
      }
      String msg = "Unable to create index: ";
      LOG.error(msg, e);
      if ((e instanceof TException)) {
        throw e;
      }
      throw new MetaException(msg + e);
    }
    return index;
  }

  @Override
  public void alter_index(String dbName, String tblName, String indexName, Index index)
    throws InvalidOperationException, MetaException, TException {
    com.amazonaws.services.glue.model.Table catalogIndexTableObject = HiveToCatalogConverter
        .convertIndexToTableObject(index);
    Table originTable = get_table(dbName, tblName);
    String indexTableObjectName = "index_prefix" + indexName;
    if (!originTable.getParameters().containsKey(indexTableObjectName)) {
      throw new NoSuchObjectException("can not find index: " + indexName);
    }
    originTable.getParameters().put(indexTableObjectName, ConverterUtils.catalogTableToString(catalogIndexTableObject));
    alter_table(dbName, tblName, originTable);
  }

  @Override
  public boolean drop_index_by_name(String db_name, String tbl_name, String index_name, boolean deleteData)
    throws NoSuchObjectException, MetaException, TException {
    return this.dropIndex(db_name, tbl_name, index_name, deleteData);
  }

  private boolean dropIndex(String dbName, String tblName, String name, boolean deleteData)
    throws NoSuchObjectException, MetaException, TException {
    Index indexToDrop = get_index_by_name(dbName, tblName, name);
    String indexTableName = indexToDrop.getIndexTableName();

    Table originTable = get_table(dbName, tblName);
    Map<String, String> parameters = originTable.getParameters();
    String indexTableObjectName = "index_prefix" + name;
    if (!parameters.containsKey(indexTableObjectName)) {
      throw new NoSuchObjectException("can not find Index: " + name);
    }
    parameters.remove(indexTableObjectName);

    alter_table(dbName, tblName, originTable);
    if ((indexTableName != null) && (indexTableName.length() > 0)) {
      drop_table(dbName, indexTableName, deleteData);
    }
    return true;
  }

  @Override
  public Index get_index_by_name(String dbName, String tblName, String indexName)
    throws MetaException, NoSuchObjectException, TException {
    Table originTable = get_table(dbName, tblName);
    Map<String, String> map = originTable.getParameters();
    String indexTableName = "index_prefix" + indexName;
    if (!map.containsKey(indexTableName)) {
      throw new NoSuchObjectException("can not find index: " + indexName);
    }
    com.amazonaws.services.glue.model.Table indexTableObject = ConverterUtils
        .stringToCatalogTable((String) map.get(indexTableName));
    return CatalogToHiveConverter.convertTableObjectToIndex(indexTableObject);
  }

  @Override
  public List<Index> get_indexes(String db_name, String tbl_name, short max_indexes)
    throws NoSuchObjectException, MetaException, TException {
    return this.listIndexes(db_name, tbl_name, max_indexes);
  }

  private List<Index> listIndexes(String db_name, String tbl_name, short max)
    throws NoSuchObjectException, MetaException, TException {
    return this.glueMetastoreClientDelegate.listIndexes(db_name, tbl_name);
  }

  @Override
  public List<String> get_index_names(String db_name, String tbl_name, short max_indexes)
    throws MetaException, TException {
    return listIndexNames(db_name, tbl_name, max_indexes);
  }

  private List<String> listIndexNames(String db_name, String tbl_name, short max) throws MetaException, TException {
    List<Index> indexes = listIndexes(db_name, tbl_name, max);
    List<String> indexNames = Lists.newArrayList();
    for (Index index : indexes) {
      indexNames.add(index.getIndexName());
    }
    return indexNames;
  }

  @Override
  public boolean update_table_column_statistics(ColumnStatistics stats_obj)
    throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    return this.glueMetastoreClientDelegate.updateTableColumnStatistics(stats_obj);
  }

  @Override
  public boolean update_partition_column_statistics(ColumnStatistics stats_obj)
    throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    return this.glueMetastoreClientDelegate.updatePartitionColumnStatistics(stats_obj);
  }

  @Override
  public ColumnStatistics get_table_column_statistics(String db_name, String tbl_name, String col_name)
    throws NoSuchObjectException, MetaException, InvalidInputException, InvalidObjectException, TException {
    throw new UnsupportedOperationException("get_table_column_statistics is not supported");
  }

  @Override
  public ColumnStatistics get_partition_column_statistics(
      String db_name,
      String tbl_name,
      String part_name,
      String col_name)
    throws NoSuchObjectException, MetaException, InvalidInputException, InvalidObjectException, TException {
    throw new UnsupportedOperationException("get_partition_column_statistics is not supported");
  }

  @Override
  public TableStatsResult get_table_statistics_req(TableStatsRequest request)
    throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("get_table_statistics_req is not supported");
  }

  @Override
  public PartitionsStatsResult get_partitions_statistics_req(PartitionsStatsRequest request)
    throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("get_partition_statistics_req is not supported");
  }

  @Override
  public AggrStats get_aggr_stats_for(PartitionsStatsRequest request)
    throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("get_aggr_stats_for is not supported");
  }

  @Override
  public boolean set_aggr_stats_for(SetPartitionsStatsRequest request)
    throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    throw new UnsupportedOperationException("set_aggr_stats_for is not supported");
  }

  @Override
  public boolean delete_partition_column_statistics(String db_name, String tbl_name, String part_name, String col_name)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException, TException {
    return this.glueMetastoreClientDelegate.deletePartitionColumnStatistics(db_name, tbl_name, part_name, col_name);
  }

  @Override
  public boolean delete_table_column_statistics(String db_name, String tbl_name, String col_name)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException, TException {
    return this.glueMetastoreClientDelegate.deleteTableColumnStatistics(db_name, tbl_name, col_name);
  }

  @Override
  public void create_function(Function function)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    try {
      UserDefinedFunctionInput functionInput = GlueInputConverter.convertToUserDefinedFunctionInput(function);
      this.glueClient.createUserDefinedFunction(
          new CreateUserDefinedFunctionRequest().withDatabaseName(function.getDbName()).withFunctionInput(
              functionInput));
    } catch (AmazonServiceException e) {
      LOG.error(e.toString());
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to create Function: ";
      LOG.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  @Override
  public void drop_function(String dbName, String functionName)
    throws NoSuchObjectException, MetaException, TException {
    try {
      this.glueClient.deleteUserDefinedFunction(
          new DeleteUserDefinedFunctionRequest().withDatabaseName(dbName).withFunctionName(functionName));
    } catch (AmazonServiceException e) {
      LOG.error(e.toString());
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to drop Function: ";
      LOG.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  @Override
  public void alter_function(String dbName, String functionName, Function newFunction)
    throws InvalidObjectException, MetaException, TException {
    try {
      UserDefinedFunctionInput functionInput = GlueInputConverter.convertToUserDefinedFunctionInput(newFunction);
      this.glueClient.updateUserDefinedFunction(new UpdateUserDefinedFunctionRequest()
          .withDatabaseName(dbName)
          .withFunctionName(functionName)
          .withFunctionInput(functionInput));
    } catch (AmazonServiceException e) {
      LOG.error(e.toString());
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to alter Function: ";
      LOG.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  @Override
  public List<String> get_functions(String dbName, String pattern) throws MetaException, TException {
    try {
      List<String> functionNames = Lists.newArrayList();
      String nextToken = null;
      do {
        GetUserDefinedFunctionsResult result = this.glueClient
            .getUserDefinedFunctions(new GetUserDefinedFunctionsRequest()

                .withDatabaseName(dbName)
                .withPattern(pattern)
                .withNextToken(nextToken));
        nextToken = result.getNextToken();
        for (UserDefinedFunction catalogFunction : result.getUserDefinedFunctions()) {
          functionNames.add(catalogFunction.getFunctionName());
        }
      } while (nextToken != null);
      return functionNames;
    } catch (AmazonServiceException e) {
      LOG.error(e.toString());
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get Functions: ";
      LOG.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  @Override
  public Function get_function(String dbName, String functionName) throws MetaException, TException {
    try {
      GetUserDefinedFunctionResult result = this.glueClient.getUserDefinedFunction(new GetUserDefinedFunctionRequest()

          .withDatabaseName(dbName)
          .withFunctionName(functionName));
      return CatalogToHiveConverter.convertFunction(dbName, result.getUserDefinedFunction());
    } catch (AmazonServiceException e) {
      LOG.error(e.toString());
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get Function: ";
      LOG.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  @Override
  public boolean create_role(Role role) throws MetaException, TException {
    return this.glueMetastoreClientDelegate.createRole(role);
  }

  @Override
  public boolean drop_role(String role_name) throws MetaException, TException {
    return this.glueMetastoreClientDelegate.dropRole(role_name);
  }

  @Override
  public List<String> get_role_names() throws MetaException, TException {
    return this.glueMetastoreClientDelegate.listRoleNames();
  }

  @Override
  public boolean grant_role(
      String roleName,
      String userName,
      PrincipalType principalType,
      String grantor,
      PrincipalType grantorType,
      boolean grantOption)
    throws MetaException, TException {
    return this.glueMetastoreClientDelegate.grantRole(roleName, userName, principalType, grantor, grantorType,
        grantOption);
  }

  @Override
  public boolean revoke_role(String roleName, String userName, PrincipalType principalType)
    throws MetaException, TException {
    return this.glueMetastoreClientDelegate.revokeRole(roleName, userName, principalType, false);
  }

  @Override
  public List<Role> list_roles(String principalName, PrincipalType principalType) throws MetaException, TException {
    return this.glueMetastoreClientDelegate.listRoles(principalName, principalType);
  }

  @Override
  public GrantRevokeRoleResponse grant_revoke_role(GrantRevokeRoleRequest request) throws MetaException, TException {
    boolean revoked = revoke_role(request.getRoleName(), request.getGrantor(), request.getPrincipalType(), false);
    GrantRevokeRoleResponse revokeRoleResponse = new GrantRevokeRoleResponse();
    revokeRoleResponse.setSuccess(revoked);
    return revokeRoleResponse;
  }

  private boolean revoke_role(String roleName, String userName, PrincipalType principalType, boolean grantOption)
    throws MetaException, TException {
    return this.glueMetastoreClientDelegate.revokeRole(roleName, userName, principalType, grantOption);
  }

  @Override
  public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest request)
    throws MetaException, TException {
    return this.glueMetastoreClientDelegate.getPrincipalsInRole(request);
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(GetRoleGrantsForPrincipalRequest request)
    throws MetaException, TException {
    return this.glueMetastoreClientDelegate.getRoleGrantsForPrincipal(request);
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String user_name, List<String> group_names)
    throws MetaException, TException {
    return this.glueMetastoreClientDelegate.getPrivilegeSet(hiveObject, user_name, group_names);
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(
      String principal_name,
      PrincipalType principal_type,
      HiveObjectRef hiveObject)
    throws MetaException, TException {
    return this.glueMetastoreClientDelegate.listPrivileges(principal_name, principal_type, hiveObject);
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privileges) throws MetaException, TException {
    return this.glueMetastoreClientDelegate.grantPrivileges(privileges);
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privileges) throws MetaException, TException {
    return this.glueMetastoreClientDelegate.revokePrivileges(privileges, false);
  }

  @Override
  public GrantRevokePrivilegeResponse grant_revoke_privileges(GrantRevokePrivilegeRequest request)
    throws MetaException, TException {
    throw new UnsupportedOperationException("grant_revoke_privileges is not supported");
  }

  @Override
  public List<String> set_ugi(String user_name, List<String> group_names) throws MetaException, TException {
    return ImmutableList.of("this_value_doesnt_matter");
  }

  @Override
  public String get_delegation_token(String token_owner, String renewer_kerberos_principal_name)
    throws MetaException, TException {
    return this.glueMetastoreClientDelegate.getDelegationToken(token_owner, renewer_kerberos_principal_name);
  }

  @Override
  public long renew_delegation_token(String token_str_form) throws MetaException, TException {
    return this.glueMetastoreClientDelegate.renewDelegationToken(token_str_form);
  }

  @Override
  public void cancel_delegation_token(String token_str_form) throws MetaException, TException {
    this.glueMetastoreClientDelegate.cancelDelegationToken(token_str_form);
  }

  @Override
  public GetOpenTxnsResponse get_open_txns() throws TException {
    throw new UnsupportedOperationException("get_open_txns is not supported");
  }

  @Override
  public GetOpenTxnsInfoResponse get_open_txns_info() throws TException {
    throw new UnsupportedOperationException("get_open_txns_info is not supported");
  }

  @Override
  public OpenTxnsResponse open_txns(OpenTxnRequest rqst) throws TException {
    throw new UnsupportedOperationException("open_txns is not supported");
  }

  @Override
  public void abort_txn(AbortTxnRequest rqst) throws NoSuchTxnException, TException {
    throw new UnsupportedOperationException("abort_txns is not supported");
  }

  @Override
  public void commit_txn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException, TException {
    throw new UnsupportedOperationException("commit_txn is not supported");
  }

  @Override
  public LockResponse lock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException, TException {
    return this.glueMetastoreClientDelegate.lock(rqst);
  }

  @Override
  public LockResponse check_lock(CheckLockRequest rqst)
    throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
    return this.glueMetastoreClientDelegate.checkLock(rqst.getLockid());
  }

  @Override
  public void unlock(UnlockRequest rqst) throws NoSuchLockException, TxnOpenException, TException {
    this.glueMetastoreClientDelegate.unlock(rqst.getLockid());
  }

  @Override
  public ShowLocksResponse show_locks(ShowLocksRequest rqst) throws TException {
    return this.glueMetastoreClientDelegate.showLocks();
  }

  @Override
  public void heartbeat(HeartbeatRequest ids)
    throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
    this.glueMetastoreClientDelegate.heartbeat(ids.getTxnid(), ids.getLockid());
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeat_txn_range(HeartbeatTxnRangeRequest txns) throws TException {
    return this.glueMetastoreClientDelegate.heartbeatTxnRange(txns.getMin(), txns.getMax());
  }

  @Override
  public void compact(CompactionRequest rqst) throws TException {
    this.glueMetastoreClientDelegate.compact(rqst.getDbname(), rqst.getTablename(), rqst.getPartitionname(),
        rqst.getType());
  }

  @Override
  public ShowCompactResponse show_compact(ShowCompactRequest rqst) throws TException {
    throw new UnsupportedOperationException("show_compact is not supported");
  }

  @Override
  public String getCpuProfile(int arg0) throws TException {
    throw new UnsupportedOperationException("getCpuProfile is not supported");
  }

  @Override
  public long aliveSince() throws TException {
    return 0;
  }

  @Override
  public String getName() throws TException {
    return null;
  }

  @Override
  public String getVersion() throws TException {
    throw new UnsupportedOperationException("getVersion is not supported");
  }

  @Override
  public fb_status getStatus() {
    throw new UnsupportedOperationException("getStatus is not supported");
  }

  @Override
  public String getStatusDetails() throws TException {
    return null;
  }

  @Override
  public Map<String, Long> getCounters() throws TException {
    return null;
  }

  @Override
  public long getCounter(String key) throws TException {
    return 0;
  }

  @Override
  public void setOption(String key, String value) throws TException {}

  @Override
  public String getOption(String key) throws TException {
    return null;
  }

  @Override
  public Map<String, String> getOptions() throws TException {
    return null;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void setConf(Configuration conf) {
    try {
      this.conf = HCatUtil.getHiveConf(conf);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void init() throws MetaException {}

  @Override
  public Type get_type(String name) throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException("get_type is not supported");
  }

  @Override
  public boolean create_type(Type type)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException("create_type is not supported");
  }

  @Override
  public boolean drop_type(String type) throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException("drop_type is not supported");
  }

  @Override
  public Map<String, Type> get_type_all(String name) throws MetaException, TException {
    throw new UnsupportedOperationException("get_type_all is not supported");
  }

  // 2.1

  @Override
  public void abort_txns(AbortTxnsRequest rqst) throws NoSuchTxnException, TException {
    this.glueMetastoreClientDelegate.abortTxns(rqst.getTxn_ids());
  }

  @Override
  public void add_dynamic_partitions(AddDynamicPartitions rqst)
    throws NoSuchTxnException, TxnAbortedException, TException {
    this.glueMetastoreClientDelegate.addDynamicPartitions(rqst.getTxnid(), rqst.getDbname(), rqst.getTablename(),
        rqst.getPartitionnames(), rqst.getOperationType());
  }

  @Override
  public void add_foreign_key(AddForeignKeyRequest req) throws NoSuchObjectException, MetaException, TException {
    this.glueMetastoreClientDelegate.addForeignKey(req.getForeignKeyCols());
  }

  @Override
  public int add_master_key(String key) throws MetaException, TException {
    return this.glueMetastoreClientDelegate.addMasterKey(key);
  }

  @Override
  public void add_primary_key(AddPrimaryKeyRequest req) throws NoSuchObjectException, MetaException, TException {
    this.glueMetastoreClientDelegate.addPrimaryKey(req.getPrimaryKeyCols());
  }

  @Override
  public boolean add_token(String token_identifier, String delegation_token) throws TException {
    return this.glueMetastoreClientDelegate.addToken(token_identifier, delegation_token);
  }

  @Override
  public void alter_partitions_with_environment_context(
      String db_name,
      String tbl_name,
      List<Partition> new_parts,
      EnvironmentContext environment_context)
    throws InvalidOperationException, MetaException, TException {
    this.glueMetastoreClientDelegate.alterPartitions(db_name, tbl_name, new_parts);
  }

  @Override
  public void alter_table_with_cascade(String dbname, String tbl_name, Table new_tbl, boolean cascade)
    throws InvalidOperationException, MetaException, TException {
    this.glueMetastoreClientDelegate.alterTable(dbname, tbl_name, new_tbl, null);
  }

  @Override
  public CacheFileMetadataResult cache_file_metadata(CacheFileMetadataRequest req) throws TException {
    throw new UnsupportedOperationException("cache_file_metadata is not supported");
  }

  @Override
  public ClearFileMetadataResult clear_file_metadata(ClearFileMetadataRequest req) throws TException {
    throw new UnsupportedOperationException("clear_file_metadata is not supported");
  }

  @Override
  public void create_table_with_constraints(Table tbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    glueMetastoreClientDelegate.createTableWithConstraints(tbl, primaryKeys, foreignKeys);
  }

  @Override
  public void drop_constraint(DropConstraintRequest req) throws NoSuchObjectException, MetaException, TException {
    this.glueMetastoreClientDelegate.dropConstraint(req.getDbname(), req.getTablename(), req.getConstraintname());
  }

  @Override
  public List<Partition> exchange_partitions(
      Map<String, String> partitionSpecs,
      String source_db,
      String source_table_name,
      String dest_db,
      String dest_table_name)
    throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    return this.glueMetastoreClientDelegate.exchangePartitions(partitionSpecs, source_db, source_table_name, dest_db,
        dest_table_name);
  }

  @Override
  public FireEventResponse fire_listener_event(FireEventRequest rqst) throws TException {
    return this.glueMetastoreClientDelegate.fireListenerEvent(rqst);
  }

  @Override
  public void flushCache() throws TException {
    throw new UnsupportedOperationException("flushCache is not supported");
  }

  @Override
  public GetAllFunctionsResponse get_all_functions() throws MetaException, TException {
    List<String> databaseNames = get_databases(".*");
    List<Function> result = new ArrayList();
    try {
      for (Iterator localIterator1 = databaseNames.iterator(); localIterator1.hasNext();) {
        String databaseName = (String) localIterator1.next();

        List<UserDefinedFunction> catalogFunctions = this.glueClient
            .getUserDefinedFunctions(
                new GetUserDefinedFunctionsRequest().withDatabaseName(databaseName).withPattern(".*"))
            .getUserDefinedFunctions();
        for (UserDefinedFunction catalogFunction : catalogFunctions) {
          result.add(CatalogToHiveConverter.convertFunction(databaseName, catalogFunction));
        }
      }
      String databaseName;
      GetAllFunctionsResponse response = new GetAllFunctionsResponse();
      response.setFunctions(result);
      return response;
    } catch (AmazonServiceException e) {
      LOG.error(e.toString());
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get Functions: ";
      LOG.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  @Override
  public List<String> get_all_token_identifiers() throws TException {
    return this.glueMetastoreClientDelegate.getAllTokenIdentifiers();
  }

  @Override
  public CurrentNotificationEventId get_current_notificationEventId() throws TException {
    return this.glueMetastoreClientDelegate.getCurrentNotificationEventId();
  }

  @Override
  public List<FieldSchema> get_fields_with_environment_context(
      String db_name,
      String table_name,
      EnvironmentContext environment_context)
    throws MetaException, UnknownTableException, UnknownDBException, TException {
    return this.glueMetastoreClientDelegate.getFields(db_name, table_name);
  }

  @Override
  public GetFileMetadataResult get_file_metadata(GetFileMetadataRequest req) throws TException {
    throw new UnsupportedOperationException("get_file_metadata is not supported");
  }

  @Override
  public GetFileMetadataByExprResult get_file_metadata_by_expr(GetFileMetadataByExprRequest req) throws TException {
    throw new UnsupportedOperationException("get_file_metadata_by_expr is not supported");

  }

  @Override
  public ForeignKeysResponse get_foreign_keys(ForeignKeysRequest request)
    throws MetaException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public List<String> get_master_keys() throws TException {
    return Arrays.asList(this.glueMetastoreClientDelegate.getMasterKeys());
  }

  @Override
  public NotificationEventResponse get_next_notification(NotificationEventRequest rqst) throws TException {
    throw new UnsupportedOperationException("get_next_notification is not supported");
  }

  @Override
  public int get_num_partitions_by_filter(String db_name, String tbl_name, String filter)
    throws MetaException, NoSuchObjectException, TException {
    return this.glueMetastoreClientDelegate.getNumPartitionsByFilter(db_name, tbl_name, filter);
  }

  @Override
  public PrimaryKeysResponse get_primary_keys(PrimaryKeysRequest request)
    throws MetaException, NoSuchObjectException, TException {
    return null;
  }

  @Override
  public List<FieldSchema> get_schema_with_environment_context(
      String db_name,
      String table_name,
      EnvironmentContext environment_context)
    throws MetaException, UnknownTableException, UnknownDBException, TException {
    return this.glueMetastoreClientDelegate.getSchema(db_name, table_name);
  }

  @Override
  public List<TableMeta> get_table_meta(String db_patterns, String tbl_patterns, List<String> tbl_types)
    throws MetaException, TException {
    return this.glueMetastoreClientDelegate.getTableMeta(db_patterns, tbl_patterns, tbl_types);
  }

  @Override
  public String get_token(String token_identifier) throws TException {
    return this.glueMetastoreClientDelegate.getToken(token_identifier);
  }

  @Override
  public PutFileMetadataResult put_file_metadata(PutFileMetadataRequest req) throws TException {
    throw new UnsupportedOperationException("put_file_metadata is not supported");
  }

  @Override
  public boolean remove_master_key(int key_seq) throws TException {
    return this.glueMetastoreClientDelegate.removeMasterKey(key_seq);
  }

  @Override
  public boolean remove_token(String token_identifier) throws TException {
    return this.glueMetastoreClientDelegate.removeToken(token_identifier);
  }

  @Override
  public void update_master_key(int seq_number, String key) throws NoSuchObjectException, MetaException, TException {
    this.glueMetastoreClientDelegate.updateMasterKey(seq_number, key);
  }

  // Hive 2.3.0 methods

  @Override
  public List<String> get_tables_by_type(String db_name, String pattern, String tableType)
    throws MetaException, TException {
    throw new UnsupportedOperationException("get_tables_by_type is not supported");
  }

  @Override
  public GetTableResult get_table_req(GetTableRequest req) throws MetaException, NoSuchObjectException, TException {
    Table table = this.glueMetastoreClientDelegate.getTable(req.getDbName(), req.getTblName());
    GetTableResult result = new GetTableResult();
    result.setTable(table);
    return result;
  }

  @Override
  public GetTablesResult get_table_objects_by_name_req(GetTablesRequest req)
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    List<Table> tables = this.getTableObjectsByName(req.getDbName(), req.getTblNames());
    GetTablesResult results = new GetTablesResult();
    results.setTables(tables);
    return results;
  }

  private List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    List<Table> hiveTables = Lists.newArrayList();
    for (String tableName : tableNames) {
      hiveTables.add(get_table(dbName, tableName));
    }
    return hiveTables;
  }

  @Override
  public CompactionResponse compact2(CompactionRequest rqst) throws TException {
    return this.glueMetastoreClientDelegate.compact2(rqst.getDbname(), rqst.getTablename(), rqst.getPartitionname(),
        rqst.getType(), rqst.getProperties());
  }
}
