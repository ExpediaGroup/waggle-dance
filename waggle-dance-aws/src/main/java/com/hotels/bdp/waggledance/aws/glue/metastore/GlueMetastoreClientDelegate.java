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
package com.hotels.bdp.waggledance.aws.glue.metastore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.CreateDatabaseRequest;
import com.amazonaws.services.glue.model.CreateTableRequest;
import com.amazonaws.services.glue.model.DatabaseInput;
import com.amazonaws.services.glue.model.DeleteDatabaseRequest;
import com.amazonaws.services.glue.model.DeletePartitionRequest;
import com.amazonaws.services.glue.model.DeleteTableRequest;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetDatabaseRequest;
import com.amazonaws.services.glue.model.GetDatabaseResult;
import com.amazonaws.services.glue.model.GetDatabasesRequest;
import com.amazonaws.services.glue.model.GetDatabasesResult;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.GetPartitionsRequest;
import com.amazonaws.services.glue.model.GetPartitionsResult;
import com.amazonaws.services.glue.model.GetTableRequest;
import com.amazonaws.services.glue.model.GetTableResult;
import com.amazonaws.services.glue.model.GetTablesRequest;
import com.amazonaws.services.glue.model.GetTablesResult;
import com.amazonaws.services.glue.model.PartitionInput;
import com.amazonaws.services.glue.model.TableInput;
import com.amazonaws.services.glue.model.UpdateDatabaseRequest;
import com.amazonaws.services.glue.model.UpdatePartitionRequest;
import com.amazonaws.services.glue.model.UpdateTableRequest;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.hotels.bdp.waggledance.aws.glue.converter.CatalogToHiveConverter;
import com.hotels.bdp.waggledance.aws.glue.converter.ConverterUtils;
import com.hotels.bdp.waggledance.aws.glue.converter.GlueInputConverter;
import com.hotels.bdp.waggledance.aws.glue.converter.HiveToCatalogConverter;
import com.hotels.bdp.waggledance.aws.glue.converter.util.BatchCreatePartitionsHelper;
import com.hotels.bdp.waggledance.aws.glue.converter.util.ExpressionHelper;
import com.hotels.bdp.waggledance.aws.glue.converter.util.MetastoreClientUtils;
import com.hotels.bdp.waggledance.aws.glue.converter.util.PartitionKey;
import com.hotels.bdp.waggledance.aws.glue.shims.AwsGlueHiveShims;
import com.hotels.bdp.waggledance.aws.glue.shims.ShimsLoader;

class GlueMetastoreClientDelegate {
  private static final Logger logger = Logger.getLogger(GlueMetastoreClientDelegate.class);

  private static final List<Role> implicitRoles = Lists.newArrayList(new Role[] { new Role("public", 0, "public") });
  public static final int MILLISECOND_TO_SECOND_FACTOR = 1000;
  public static final Long NO_MAX = Long.valueOf(-1L);

  public static final String MATCH_ALL = ".*";

  public static final String INDEX_PREFIX = "index_prefix";
  private static final int BATCH_CREATE_PARTITIONS_PAGE_SIZE = 100;
  private static final int NUM_EXECUTOR_THREADS = 5;
  static final String BATCH_CREATE_PARTITIONS_THREAD_POOL_NAME_FORMAT = "batch-create-partitions-%d";
  private static final ExecutorService BATCH_CREATE_PARTITIONS_THREAD_POOL = Executors.newFixedThreadPool(5,
      new ThreadFactoryBuilder().setNameFormat("batch-create-partitions-%d").setDaemon(true).build());

  private final AWSGlue glueClient;

  private final HiveConf conf;
  private final Warehouse warehouse;
  private final AwsGlueHiveShims hiveShims = ShimsLoader.getHiveShims();

  GlueMetastoreClientDelegate(HiveConf conf, AWSGlue glueClient, Warehouse warehouse) throws MetaException {
    Preconditions.checkNotNull(conf, "Hive Config cannot be null");
    Preconditions.checkNotNull(glueClient, "glueClient cannot be null");
    Preconditions.checkNotNull(warehouse, "Warehouse cannot be null");
    this.conf = conf;
    this.glueClient = glueClient;
    this.warehouse = warehouse;
  }

  public void createDatabase(org.apache.hadoop.hive.metastore.api.Database database) throws TException {
    Preconditions.checkNotNull(database, "database cannot be null");

    if (StringUtils.isEmpty(database.getLocationUri())) {
      database.setLocationUri(warehouse.getDefaultDatabasePath(database.getName()).toString());
    } else {
      database.setLocationUri(warehouse.getDnsPath(new Path(database.getLocationUri())).toString());
    }
    Path dbPath = new Path(database.getLocationUri());
    boolean madeDir = MetastoreClientUtils.makeDirs(warehouse, dbPath);
    try {
      DatabaseInput catalogDatabase = GlueInputConverter.convertToDatabaseInput(database);
      glueClient.createDatabase(new CreateDatabaseRequest().withDatabaseInput(catalogDatabase));
    } catch (AmazonServiceException e) {
      if (madeDir) {
        warehouse.deleteDir(dbPath, true);
      }
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to create database: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public List<FieldSchema> getSchema(String db, String tableName)
    throws MetaException, TException, UnknownTableException, UnknownDBException {
    try {
      GetTableResult result = this.glueClient.getTable(new GetTableRequest().withDatabaseName(db).withName(tableName));
      com.amazonaws.services.glue.model.Table table = result.getTable();
      List<Column> schemas = table.getStorageDescriptor().getColumns();
      if ((table.getPartitionKeys() != null) && (!table.getPartitionKeys().isEmpty())) {
        schemas.addAll(table.getPartitionKeys());
      }
      return CatalogToHiveConverter.convertFieldSchemaList(schemas);
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get field from table: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public List<FieldSchema> getFields(String db_name, String table_name)
    throws MetaException, UnknownTableException, UnknownDBException, TException {
    try {
      GetTableResult result = this.glueClient.getTable(
          new GetTableRequest().withDatabaseName(db_name).withName(table_name));
      com.amazonaws.services.glue.model.Table table = result.getTable();
      return CatalogToHiveConverter.convertFieldSchemaList(table.getStorageDescriptor().getColumns());
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get field from table: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public org.apache.hadoop.hive.metastore.api.Database getDatabase(String name) throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(name), "name cannot be null or empty");
    try {
      GetDatabaseResult result = glueClient.getDatabase(new GetDatabaseRequest().withName(name));
      com.amazonaws.services.glue.model.Database catalogDatabase = result.getDatabase();
      return CatalogToHiveConverter.convertDatabase(catalogDatabase);
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get database object: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public List<String> getDatabases(String pattern) throws TException {
    if ((pattern == null) || (pattern.equals("*"))) {
      pattern = ".*";
    }
    try {
      List<String> ret = Lists.newArrayList();
      String nextToken = null;
      do {
        GetDatabasesResult result = glueClient.getDatabases(new GetDatabasesRequest().withNextToken(nextToken));
        nextToken = result.getNextToken();

        for (com.amazonaws.services.glue.model.Database db : result.getDatabaseList()) {
          String name = db.getName();
          if (Pattern.matches(pattern, name)) {
            ret.add(name);
          }
        }
      } while (nextToken != null);
      return ret;
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get databases: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public void alterDatabase(String databaseName, org.apache.hadoop.hive.metastore.api.Database database)
    throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
    Preconditions.checkNotNull(database, "database cannot be null");
    try {
      DatabaseInput catalogDatabase = GlueInputConverter.convertToDatabaseInput(database);
      glueClient.updateDatabase(new UpdateDatabaseRequest().withName(databaseName).withDatabaseInput(catalogDatabase));
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to alter database: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
    throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(name), "name cannot be null or empty");

    try {
      List<String> tables = getTables(name, ".*");
      boolean isEmptyDatabase = tables.isEmpty();

      org.apache.hadoop.hive.metastore.api.Database db = getDatabase(name);
      String dbLocation = db.getLocationUri();

      if ((isEmptyDatabase) || (cascade)) {
        glueClient.deleteDatabase(new DeleteDatabaseRequest().withName(name));
      } else {
        throw new InvalidOperationException("Database " + name + " is not empty.");
      }
      if (deleteData) {
        try {
          warehouse.deleteDir(new Path(dbLocation), true);
        } catch (Exception e) {
          logger.error("Unable to remove database directory " + dbLocation, e);
        }
      }
    } catch (NoSuchObjectException e) {
      if (ignoreUnknownDb) {
        return;
      }
      throw e;
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to drop database: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public boolean databaseExists(String dbName) throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    try {
      getDatabase(dbName);
    } catch (NoSuchObjectException e) {
      return false;
    } catch (AmazonServiceException e) {
      throw new TException(e);
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
    return true;
  }

  public void createTable(org.apache.hadoop.hive.metastore.api.Table tbl) throws TException {
    Preconditions.checkNotNull(tbl, "tbl cannot be null");
    boolean dirCreated = validateNewTableAndCreateDirectory(tbl);

    try {
      tbl.setParameters(MetastoreClientUtils.deepCopyMap(tbl.getParameters()));
      tbl.getParameters().put("transient_lastDdlTime", Long.toString(System.currentTimeMillis() / 1000L));

      TableInput tableInput = GlueInputConverter.convertToTableInput(tbl);
      glueClient.createTable(new CreateTableRequest().withTableInput(tableInput).withDatabaseName(tbl.getDbName()));
    } catch (AmazonServiceException e) {
      if (dirCreated) {
        Path tblPath = new Path(tbl.getSd().getLocation());
        warehouse.deleteDir(tblPath, true);
      }
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to create table: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public boolean tableExists(String databaseName, String tableName) throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");

    if (!databaseExists(databaseName)) {
      throw new UnknownDBException("Database: " + databaseName + " does not exist.");
    }
    try {
      glueClient.getTable(new GetTableRequest().withDatabaseName(databaseName).withName(tableName));
      return true;
    } catch (EntityNotFoundException e) {
      return false;
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to check table exist: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public org.apache.hadoop.hive.metastore.api.Table getTable(String dbName, String tableName) throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");
    try {
      GetTableResult result = glueClient.getTable(new GetTableRequest().withDatabaseName(dbName).withName(tableName));
      return CatalogToHiveConverter.convertTable(result.getTable(), dbName);
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get table: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public List<String> getTables(String dbname, String tablePattern) throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(dbname), "dbName cannot be null or empty");

    List<String> names = Lists.newArrayList();
    try {
      String nextToken = null;
      do {
        GetTablesResult result = glueClient.getTables(new GetTablesRequest()

            .withDatabaseName(dbname)
            .withExpression(tablePattern)
            .withNextToken(nextToken));

        for (com.amazonaws.services.glue.model.Table catalogTable : result.getTableList()) {
          names.add(catalogTable.getName());
        }

        nextToken = result.getNextToken();
      } while (nextToken != null);
      return names;
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get tables: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
    throws TException {
    List<TableMeta> tables = new ArrayList<>();
    List<String> databases = getDatabases(dbPatterns);

    for (String dbName : databases) {
      String nextToken = null;

      do {
        GetTablesRequest request = new GetTablesRequest()
            .withDatabaseName(dbName)
            .withExpression(tablePatterns)
            .withNextToken(nextToken);
        GetTablesResult result = glueClient.getTables(request);

        for (com.amazonaws.services.glue.model.Table catalogTable : result.getTableList()) {
          if ((tableTypes == null) || (tableTypes.isEmpty()) || (tableTypes.contains(catalogTable.getTableType()))) {
            tables.add(CatalogToHiveConverter.convertTableMeta(catalogTable, dbName));
          }
        }

        nextToken = result.getNextToken();
      } while (nextToken != null);
    }

    return tables;
  }

  public void alterTable(
      String dbName,
      String oldTableName,
      org.apache.hadoop.hive.metastore.api.Table newTable,
      EnvironmentContext environmentContext)
    throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(oldTableName), "oldTableName cannot be null or empty");
    Preconditions.checkNotNull(newTable, "newTable cannot be null");

    if (isCascade(environmentContext)) {
      throw new UnsupportedOperationException("Cascade for alter_table is not supported");
    }

    if (!oldTableName.equals(newTable.getTableName())) {
      throw new UnsupportedOperationException("Table rename is not supported");
    }

    MetastoreClientUtils.validateTableObject(newTable, conf);
    if (!tableExists(dbName, oldTableName)) {
      throw new UnknownTableException("Table: " + oldTableName + " does not exists");
    }

    if ((hiveShims.requireCalStats(conf, null, null, newTable, environmentContext))
        && (newTable.getPartitionKeys().isEmpty())) {
      org.apache.hadoop.hive.metastore.api.Database db = getDatabase(newTable.getDbName());
      hiveShims.updateTableStatsFast(db, newTable, warehouse, false, true, environmentContext);
    }
    try {
      TableInput newTableInput = GlueInputConverter.convertToTableInput(newTable);
      glueClient.updateTable(new UpdateTableRequest().withDatabaseName(dbName).withTableInput(newTableInput));
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to alter table: " + oldTableName;
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  private boolean isCascade(EnvironmentContext environmentContext) {
    return (environmentContext != null)
        && (environmentContext.isSetProperties())
        && ("true".equals(environmentContext.getProperties().get("CASCADE")));
  }

  public void dropTable(String dbName, String tableName, boolean deleteData, boolean ignoreUnknownTbl, boolean ifPurge)
    throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");

    if (!tableExists(dbName, tableName)) {
      if (!ignoreUnknownTbl) {
        throw new UnknownTableException("Cannot find table: " + dbName + "." + tableName);
      }
      return;
    }

    org.apache.hadoop.hive.metastore.api.Table tbl = getTable(dbName, tableName);
    String tblLocation = tbl.getSd().getLocation();
    boolean isExternal = MetastoreClientUtils.isExternalTable(tbl);
    dropPartitionsForTable(dbName, tableName, (deleteData) && (!isExternal));
    dropIndexesForTable(dbName, tableName, (deleteData) && (!isExternal));
    try {
      glueClient.deleteTable(new DeleteTableRequest().withDatabaseName(dbName).withName(tableName));
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to drop table: ";
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }

    if ((StringUtils.isNotEmpty(tblLocation)) && (deleteData) && (!isExternal)) {
      Path tblPath = new Path(tblLocation);
      try {
        warehouse.deleteDir(tblPath, true, ifPurge);
      } catch (Exception e) {
        logger.error("Unable to remove table directory " + tblPath, e);
      }
    }
  }

  private void dropPartitionsForTable(String dbName, String tableName, boolean deleteData) throws TException {
    List<Partition> partitionsToDelete = getPartitions(dbName, tableName, null, NO_MAX.longValue());
    for (Partition part : partitionsToDelete) {
      dropPartition(dbName, tableName, part.getValues(), true, deleteData, false);
    }
  }

  private void dropIndexesForTable(String dbName, String tableName, boolean deleteData) throws TException {
    List<Index> indexesToDelete = listIndexes(dbName, tableName);
    for (Index index : indexesToDelete) {
      dropTable(dbName, index.getIndexTableName(), deleteData, true, false);
    }
  }

  public List<String> getTables(String dbname, String tablePattern, TableType tableType) throws TException {
    throw new UnsupportedOperationException("getTables with TableType is not supported");
  }

  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables) throws TException {
    throw new UnsupportedOperationException("listTableNamesByFilter is not supported");
  }

  public boolean validateNewTableAndCreateDirectory(org.apache.hadoop.hive.metastore.api.Table tbl) throws TException {
    Preconditions.checkNotNull(tbl, "tbl cannot be null");
    if (tableExists(tbl.getDbName(), tbl.getTableName())) {
      throw new AlreadyExistsException("Table " + tbl.getTableName() + " already exists.");
    }
    MetastoreClientUtils.validateTableObject(tbl, conf);

    if (TableType.VIRTUAL_VIEW.toString().equals(tbl.getTableType())) {
      return false;
    }

    if (StringUtils.isEmpty(tbl.getSd().getLocation())) {
      org.apache.hadoop.hive.metastore.api.Database db = getDatabase(tbl.getDbName());
      tbl.getSd().setLocation(hiveShims.getDefaultTablePath(db, tbl.getTableName(), warehouse).toString());
    } else {
      tbl.getSd().setLocation(warehouse.getDnsPath(new Path(tbl.getSd().getLocation())).toString());
    }

    Path tblPath = new Path(tbl.getSd().getLocation());
    return MetastoreClientUtils.makeDirs(warehouse, tblPath);
  }

  public Partition appendPartition(String dbName, String tblName, List<String> values) throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
    Preconditions.checkNotNull(values, "partition values cannot be null");
    org.apache.hadoop.hive.metastore.api.Table table = getTable(dbName, tblName);
    Preconditions.checkNotNull(table.getSd(), "StorageDescriptor cannot be null for Table " + tblName);
    Partition partition = buildPartitionFromValues(table, values);
    addPartitions(Lists.newArrayList(new Partition[] { partition }), false, true);
    return partition;
  }

  private Partition buildPartitionFromValues(org.apache.hadoop.hive.metastore.api.Table table, List<String> values)
    throws MetaException {
    Partition partition = new Partition();
    partition.setDbName(table.getDbName());
    partition.setTableName(table.getTableName());
    partition.setValues(values);
    partition.setSd(table.getSd().deepCopy());

    Path partLocation = new Path(table.getSd().getLocation(), Warehouse.makePartName(table.getPartitionKeys(), values));
    partition.getSd().setLocation(partLocation.toString());

    long timeInSecond = System.currentTimeMillis() / 1000L;
    partition.setCreateTime((int) timeInSecond);
    partition.putToParameters("transient_lastDdlTime", Long.toString(timeInSecond));
    return partition;
  }

  public List<Partition> addPartitions(List<Partition> partitions, boolean ifNotExists, boolean needResult)
    throws TException {
    Preconditions.checkNotNull(partitions, "partitions cannot be null");
    List<com.amazonaws.services.glue.model.Partition> partitionsCreated = batchCreatePartitions(partitions,
        ifNotExists);
    if (!needResult) {
      return null;
    }
    return CatalogToHiveConverter.convertPartitions(partitionsCreated);
  }

  private List<com.amazonaws.services.glue.model.Partition> batchCreatePartitions(
      List<Partition> hivePartitions,
      final boolean ifNotExists)
    throws TException {
    if (hivePartitions.isEmpty()) {
      return Lists.newArrayList();
    }

    final String dbName = ((Partition) hivePartitions.get(0)).getDbName();
    final String tableName = ((Partition) hivePartitions.get(0)).getTableName();
    org.apache.hadoop.hive.metastore.api.Table tbl = getTable(dbName, tableName);
    validateInputForBatchCreatePartitions(tbl, hivePartitions);

    List<com.amazonaws.services.glue.model.Partition> catalogPartitions = Lists.newArrayList();
    Map<PartitionKey, Path> addedPath = Maps.newHashMap();
    try {
      for (Iterator<Partition> localIterator = hivePartitions.iterator(); localIterator.hasNext();) {
        Partition partition = localIterator.next();
        Path location = getPartitionLocation(tbl, partition);
        boolean partDirCreated = false;
        if (location != null) {
          partition.getSd().setLocation(location.toString());
          partDirCreated = MetastoreClientUtils.makeDirs(warehouse, location);
        }
        com.amazonaws.services.glue.model.Partition catalogPartition = HiveToCatalogConverter
            .convertPartition(partition);
        catalogPartitions.add(catalogPartition);
        if (partDirCreated)
          addedPath.put(new PartitionKey(catalogPartition), new Path(partition.getSd().getLocation()));
      }
    } catch (MetaException e) {
      Iterator localIterator;
      Partition partition;
      for (Path path : addedPath.values()) {
        deletePath(path);
      }
      throw e;
    }

    List<Future<BatchCreatePartitionsHelper>> batchCreatePartitionsFutures = Lists.newArrayList();

    for (int i = 0; i < catalogPartitions.size(); i += 100) {
      int j = Math.min(i + 100, catalogPartitions.size());
      final List<com.amazonaws.services.glue.model.Partition> partitionsOnePage = catalogPartitions.subList(i, j);

      ((List<Future<BatchCreatePartitionsHelper>>) batchCreatePartitionsFutures)
          .add(BATCH_CREATE_PARTITIONS_THREAD_POOL.submit(new Callable() {
            public BatchCreatePartitionsHelper call() throws Exception {
              return new BatchCreatePartitionsHelper(glueClient, dbName, tableName, partitionsOnePage, ifNotExists)
                  .createPartitions();
            }
          }));
    }

    TException tException = null;
    List<com.amazonaws.services.glue.model.Partition> partitionsCreated = Lists.newArrayList();
    for (Future<BatchCreatePartitionsHelper> future : batchCreatePartitionsFutures) {
      try {
        BatchCreatePartitionsHelper batchCreatePartitionsHelper = (BatchCreatePartitionsHelper) future.get();
        partitionsCreated.addAll(batchCreatePartitionsHelper.getPartitionsCreated());
        tException = tException == null ? batchCreatePartitionsHelper.getFirstTException() : tException;
        deletePathForPartitions(batchCreatePartitionsHelper.getPartitionsFailed(), addedPath);
      } catch (Exception e) {
        logger.error("Exception thrown by BatchCreatePartitions thread pool. ", e);
      }
    }

    if (tException != null) {
      throw tException;
    }
    return partitionsCreated;
  }

  private void validateInputForBatchCreatePartitions(
      org.apache.hadoop.hive.metastore.api.Table tbl,
      List<Partition> hivePartitions) {
    Preconditions.checkNotNull(tbl.getPartitionKeys(), "Partition keys cannot be null");
    for (Partition partition : hivePartitions) {
      Preconditions.checkArgument(tbl.getDbName().equals(partition.getDbName()), "Partitions must be in the same DB");
      Preconditions.checkArgument(tbl.getTableName().equals(partition.getTableName()),
          "Partitions must be in the same table");
      Preconditions.checkNotNull(partition.getValues(), "Partition values cannot be null");
      Preconditions.checkArgument(tbl.getPartitionKeys().size() == partition.getValues().size(),
          "Number of table partition keys must match number of partition values");
    }
  }

  private void deletePathForPartitions(
      List<com.amazonaws.services.glue.model.Partition> partitions,
      Map<PartitionKey, Path> addedPath) {
    for (com.amazonaws.services.glue.model.Partition partition : partitions) {
      Path path = (Path) addedPath.get(new PartitionKey(partition));
      if (path != null) {
        deletePath(path);
      }
    }
  }

  private void deletePath(Path path) {
    try {
      warehouse.deleteDir(path, true);
    } catch (MetaException e) {
      logger.error("Warehouse delete directory failed. ", e);
    }
  }

  private Path getPartitionLocation(org.apache.hadoop.hive.metastore.api.Table tbl, Partition part)
    throws MetaException {
    Path partLocation = null;
    String partLocationStr = null;
    if (part.getSd() != null) {
      partLocationStr = part.getSd().getLocation();
    }

    if (StringUtils.isEmpty(partLocationStr)) {

      if (tbl.getSd().getLocation() != null) {
        partLocation = new Path(tbl.getSd().getLocation(),
            Warehouse.makePartName(tbl.getPartitionKeys(), part.getValues()));
      }
    } else {
      if (tbl.getSd().getLocation() == null) {
        throw new MetaException("Cannot specify location for a view partition");
      }
      partLocation = warehouse.getDnsPath(new Path(partLocationStr));
    }
    return partLocation;
  }

  public List<String> listPartitionNames(String databaseName, String tableName, List<String> values, short max)
    throws TException {
    String expression = null;
    org.apache.hadoop.hive.metastore.api.Table table = getTable(databaseName, tableName);
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

  public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(databaseName), "databaseName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(tableName), "tableName cannot be null or empty");
    Preconditions.checkNotNull(partitionNames, "partitionNames cannot be null");
    List<Partition> partitions = Lists.newArrayList();
    for (String name : partitionNames) {
      partitions.add(getPartition(databaseName, tableName, name));
    }
    return partitions;
  }

  public Partition getPartition(String dbName, String tblName, String partitionName) throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(partitionName), "partitionName cannot be null or empty");
    List<String> values = partitionNameToVals(partitionName);
    return getPartition(dbName, tblName, values);
  }

  public Partition getPartition(String dbName, String tblName, List<String> values) throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
    Preconditions.checkNotNull(values, "values cannot be null");

    GetPartitionRequest request = new GetPartitionRequest()
        .withDatabaseName(dbName)
        .withTableName(tblName)
        .withPartitionValues(values);

    try {
      GetPartitionResult res = glueClient.getPartition(request);
      com.amazonaws.services.glue.model.Partition partition = res.getPartition();
      if (partition == null) {
        logger.debug(
            "No partitions were return for dbName = " + dbName + ", tblName = " + tblName + ", values = " + values);
        return null;
      }
      return CatalogToHiveConverter.convertPartition(partition);
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to get partition with values: " + StringUtils.join(values, "/");
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }
  }

  public List<Partition> getPartitions(String databaseName, String tableName, String filter, long max)
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
        logger.error(msg, e);
        throw new MetaException(msg + e);
      }
    } while (nextToken != null);

    return partitions;
  }

  public boolean dropPartition(
      String dbName,
      String tblName,
      List<String> values,
      boolean ifExist,
      boolean deleteData,
      boolean purgeData)
    throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
    Preconditions.checkNotNull(values, "values cannot be null");

    Partition partition = null;
    try {
      partition = getPartition(dbName, tblName, values);
    } catch (NoSuchObjectException e) {
      if (ifExist) {
        return true;
      }
    }

    try {
      DeletePartitionRequest request = new DeletePartitionRequest()
          .withDatabaseName(dbName)
          .withTableName(tblName)
          .withPartitionValues(values);
      glueClient.deletePartition(request);
    } catch (AmazonServiceException e) {
      throw CatalogToHiveConverter.wrapInHiveException(e);
    } catch (Exception e) {
      String msg = "Unable to drop partition with values: " + StringUtils.join(values, "/");
      logger.error(msg, e);
      throw new MetaException(msg + e);
    }

    performDropPartitionPostProcessing(dbName, tblName, partition, deleteData, purgeData);
    return true;
  }

  private void performDropPartitionPostProcessing(
      String dbName,
      String tblName,
      Partition partition,
      boolean deleteData,
      boolean ifPurge)
    throws TException {
    if ((deleteData) && (partition.getSd() != null) && (partition.getSd().getLocation() != null)) {
      Path partPath = new Path(partition.getSd().getLocation());
      org.apache.hadoop.hive.metastore.api.Table table = getTable(dbName, tblName);
      if (MetastoreClientUtils.isExternalTable(table)) {
        return;
      }
      boolean mustPurge = isMustPurge(table, ifPurge);
      warehouse.deleteDir(partPath, true, mustPurge);
      try {
        List<String> values = partition.getValues();
        deleteParentRecursive(partPath.getParent(), values.size() - 1, mustPurge);
      } catch (IOException e) {
        throw new MetaException(e.getMessage());
      }
    }
  }

  private boolean isMustPurge(org.apache.hadoop.hive.metastore.api.Table table, boolean ifPurge) {
    return (ifPurge) || ("true".equalsIgnoreCase((String) table.getParameters().get("auto.purge")));
  }

  private void deleteParentRecursive(Path parent, int depth, boolean mustPurge) throws IOException, MetaException {
    if ((depth > 0) && (parent != null) && (warehouse.isWritable(parent)) && (warehouse.isEmpty(parent))) {
      warehouse.deleteDir(parent, true, mustPurge);
      deleteParentRecursive(parent.getParent(), depth - 1, mustPurge);
    }
  }

  public void alterPartitions(String dbName, String tblName, List<Partition> partitions) throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");
    Preconditions.checkNotNull(partitions, "partitions cannot be null");

    for (Partition part : partitions) {
      part.setParameters(MetastoreClientUtils.deepCopyMap(part.getParameters()));
      if ((part.getParameters().get("transient_lastDdlTime") == null)
          || (Integer.parseInt((String) part.getParameters().get("transient_lastDdlTime")) == 0)) {
        part.putToParameters("transient_lastDdlTime", Long.toString(System.currentTimeMillis() / 1000L));
      }

      PartitionInput partitionInput = GlueInputConverter.convertToPartitionInput(part);

      UpdatePartitionRequest request = new UpdatePartitionRequest()
          .withDatabaseName(dbName)
          .withTableName(tblName)
          .withPartitionInput(partitionInput)
          .withPartitionValueList(part.getValues());
      try {
        glueClient.updatePartition(request);
      } catch (AmazonServiceException e) {
        throw CatalogToHiveConverter.wrapInHiveException(e);
      } catch (Exception e) {
        String msg = "Unable to alter partition: ";
        logger.error(msg, e);
        throw new MetaException(msg + e);
      }
    }
  }

  public List<String> partitionNameToVals(String name) throws TException {
    Preconditions.checkNotNull(name, "name cannot be null");
    if (name.isEmpty()) {
      return Lists.newArrayList();
    }
    LinkedHashMap<String, String> map = Warehouse.makeSpecFromName(name);
    List<String> vals = Lists.newArrayList();
    vals.addAll(map.values());
    return vals;
  }

  public List<Index> listIndexes(String dbName, String tblName) throws TException {
    Preconditions.checkArgument(StringUtils.isNotEmpty(dbName), "dbName cannot be null or empty");
    Preconditions.checkArgument(StringUtils.isNotEmpty(tblName), "tblName cannot be null or empty");

    org.apache.hadoop.hive.metastore.api.Table originTable = getTable(dbName, tblName);
    Map<String, String> parameters = originTable.getParameters();
    List<com.amazonaws.services.glue.model.Table> indexTableObjects = Lists.newArrayList();
    for (Iterator<String> localIterator = parameters.keySet().iterator(); localIterator.hasNext();) {
      String key = localIterator.next();
      if (key.startsWith("index_prefix")) {
        String serialisedString = (String) parameters.get(key);
        indexTableObjects.add(ConverterUtils.stringToCatalogTable(serialisedString));
      }
    }
    String key;
    List<Index> hiveIndexList = Lists.newArrayList();
    for (com.amazonaws.services.glue.model.Table catalogIndexTableObject : indexTableObjects) {
      ((List) hiveIndexList).add(CatalogToHiveConverter.convertTableObjectToIndex(catalogIndexTableObject));
    }
    return hiveIndexList;
  }

  public boolean createRole(Role role) throws TException {
    throw new UnsupportedOperationException("createRole is not supported");
  }

  public boolean dropRole(String roleName) throws TException {
    throw new UnsupportedOperationException("dropRole is not supported");
  }

  public List<Role> listRoles(String principalName, PrincipalType principalType) throws TException {
    if (principalType == PrincipalType.USER) {
      return implicitRoles;
    }
    throw new UnsupportedOperationException(
        "listRoles is only supported for " + PrincipalType.USER + " Principal type");
  }

  public List<String> listRoleNames() throws TException {
    return Lists.newArrayList(new String[] { "public" });
  }

  public GetPrincipalsInRoleResponse getPrincipalsInRole(GetPrincipalsInRoleRequest request) throws TException {
    throw new UnsupportedOperationException("getPrincipalsInRole is not supported");
  }

  public GetRoleGrantsForPrincipalResponse getRoleGrantsForPrincipal(GetRoleGrantsForPrincipalRequest request)
    throws TException {
    throw new UnsupportedOperationException("getRoleGrantsForPrincipal is not supported");
  }

  public boolean grantRole(
      String roleName,
      String userName,
      PrincipalType principalType,
      String grantor,
      PrincipalType grantorType,
      boolean grantOption)
    throws TException {
    throw new UnsupportedOperationException("grantRole is not supported");
  }

  public boolean revokeRole(String roleName, String userName, PrincipalType principalType, boolean grantOption)
    throws TException {
    throw new UnsupportedOperationException("revokeRole is not supported");
  }

  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption) throws TException {
    throw new UnsupportedOperationException("revokePrivileges is not supported");
  }

  public boolean grantPrivileges(PrivilegeBag privileges) throws TException {
    throw new UnsupportedOperationException("grantPrivileges is not supported");
  }

  public PrincipalPrivilegeSet getPrivilegeSet(HiveObjectRef objectRef, String user, List<String> groups)
    throws TException {
    return null;
  }

  public List<HiveObjectPrivilege> listPrivileges(
      String principal,
      PrincipalType principalType,
      HiveObjectRef objectRef)
    throws TException {
    throw new UnsupportedOperationException("listPrivileges is not supported");
  }

  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName, String colName)
    throws TException {
    throw new UnsupportedOperationException("deletePartitionColumnStatistics is not supported");
  }

  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName) throws TException {
    throw new UnsupportedOperationException("deleteTableColumnStatistics is not supported");
  }

  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String dbName,
      String tableName,
      List<String> partitionNames,
      List<String> columnNames)
    throws TException {
    throw new UnsupportedOperationException("getPartitionColumnStatistics is not supported");
  }

  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName, List<String> colNames)
    throws TException {
    throw new UnsupportedOperationException("getTableColumnStatistics is not supported");
  }

  public boolean updatePartitionColumnStatistics(ColumnStatistics columnStatistics) throws TException {
    throw new UnsupportedOperationException("updatePartitionColumnStatistics is not supported");
  }

  public boolean updateTableColumnStatistics(ColumnStatistics columnStatistics) throws TException {
    throw new UnsupportedOperationException("updateTableColumnStatistics is not supported");
  }

  public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames, List<String> partName)
    throws TException {
    throw new UnsupportedOperationException("getAggrColStatsFor is not supported");
  }

  public void cancelDelegationToken(String tokenStrForm) throws TException {
    throw new UnsupportedOperationException("cancelDelegationToken is not supported");
  }

  public String getTokenStrForm() throws IOException {
    throw new UnsupportedOperationException("getTokenStrForm is not supported");
  }

  public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
    throw new UnsupportedOperationException("addToken is not supported");
  }

  public boolean removeToken(String tokenIdentifier) throws TException {
    throw new UnsupportedOperationException("removeToken is not supported");
  }

  public String getToken(String tokenIdentifier) throws TException {
    throw new UnsupportedOperationException("getToken is not supported");
  }

  public List<String> getAllTokenIdentifiers() throws TException {
    throw new UnsupportedOperationException("getAllTokenIdentifiers is not supported");
  }

  public int addMasterKey(String key) throws TException {
    throw new UnsupportedOperationException("addMasterKey is not supported");
  }

  public void updateMasterKey(Integer seqNo, String key) throws TException {
    throw new UnsupportedOperationException("updateMasterKey is not supported");
  }

  public boolean removeMasterKey(Integer keySeq) throws TException {
    throw new UnsupportedOperationException("removeMasterKey is not supported");
  }

  public String[] getMasterKeys() throws TException {
    throw new UnsupportedOperationException("getMasterKeys is not supported");
  }

  public LockResponse checkLock(long lockId) throws TException {
    throw new UnsupportedOperationException("checkLock is not supported");
  }

  public void commitTxn(long txnId) throws TException {
    throw new UnsupportedOperationException("commitTxn is not supported");
  }

  public void abortTxns(List<Long> txnIds) throws TException {
    throw new UnsupportedOperationException("abortTxns is not supported");
  }

  public void compact(String dbName, String tblName, String partitionName, CompactionType compactionType)
    throws TException {
    throw new UnsupportedOperationException("compact is not supported");
  }

  public void compact(
      String dbName,
      String tblName,
      String partitionName,
      CompactionType compactionType,
      Map<String, String> tblProperties)
    throws TException {
    throw new UnsupportedOperationException("compact is not supported");
  }

  public CompactionResponse compact2(
      String dbName,
      String tblName,
      String partitionName,
      CompactionType compactionType,
      Map<String, String> tblProperties)
    throws TException {
    throw new UnsupportedOperationException("compact2 is not supported");
  }

  public ValidTxnList getValidTxns() throws TException {
    throw new UnsupportedOperationException("getValidTxns is not supported");
  }

  public ValidTxnList getValidTxns(long currentTxn) throws TException {
    throw new UnsupportedOperationException("getValidTxns is not supported");
  }

  public Partition exchangePartition(
      Map<String, String> partitionSpecs,
      String srcDb,
      String srcTbl,
      String dstDb,
      String dstTbl)
    throws TException {
    throw new UnsupportedOperationException("exchangePartition not yet supported.");
  }

  public List<Partition> exchangePartitions(
      Map<String, String> partitionSpecs,
      String sourceDb,
      String sourceTbl,
      String destDb,
      String destTbl)
    throws TException {
    throw new UnsupportedOperationException("exchangePartitions is not yet supported");
  }

  public String getDelegationToken(String owner, String renewerKerberosPrincipalName) throws TException {
    throw new UnsupportedOperationException("getDelegationToken is not supported");
  }

  public void heartbeat(long txnId, long lockId) throws TException {
    throw new UnsupportedOperationException("heartbeat is not supported");
  }

  public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
    throw new UnsupportedOperationException("heartbeatTxnRange is not supported");
  }

  public boolean isPartitionMarkedForEvent(
      String dbName,
      String tblName,
      Map<String, String> partKVs,
      PartitionEventType eventType)
    throws TException {
    throw new UnsupportedOperationException("isPartitionMarkedForEvent is not supported");
  }

  public int getNumPartitionsByFilter(String dbName, String tableName, String filter) throws TException {
    throw new UnsupportedOperationException("getNumPartitionsByFilter is not supported.");
  }

  public PartitionSpecProxy listPartitionSpecs(String dbName, String tblName, int max) throws TException {
    throw new UnsupportedOperationException("listPartitionSpecs is not supported.");
  }

  public PartitionSpecProxy listPartitionSpecsByFilter(String dbName, String tblName, String filter, int max)
    throws TException {
    throw new UnsupportedOperationException("listPartitionSpecsByFilter is not supported");
  }

  public LockResponse lock(LockRequest lockRequest) throws TException {
    throw new UnsupportedOperationException("lock is not supported");
  }

  public void markPartitionForEvent(
      String dbName,
      String tblName,
      Map<String, String> partKeyValues,
      PartitionEventType eventType)
    throws TException {
    throw new UnsupportedOperationException("markPartitionForEvent is not supported");
  }

  public long openTxn(String user) throws TException {
    throw new UnsupportedOperationException("openTxn is not supported");
  }

  public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
    throw new UnsupportedOperationException("openTxns is not supported");
  }

  public long renewDelegationToken(String tokenStrForm) throws TException {
    throw new UnsupportedOperationException("renewDelegationToken is not supported");
  }

  public void rollbackTxn(long txnId) throws TException {
    throw new UnsupportedOperationException("rollbackTxn is not supported");
  }

  public void createTableWithConstraints(
      org.apache.hadoop.hive.metastore.api.Table table,
      List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys)
    throws AlreadyExistsException, TException {
    throw new UnsupportedOperationException("createTableWithConstraints is not supported");
  }

  public void dropConstraint(String dbName, String tblName, String constraintName) throws TException {
    throw new UnsupportedOperationException("dropConstraint is not supported");
  }

  public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols) throws TException {
    throw new UnsupportedOperationException("addPrimaryKey is not supported");
  }

  public void addForeignKey(List<SQLForeignKey> foreignKeyCols) throws TException {
    throw new UnsupportedOperationException("addForeignKey is not supported");
  }

  public ShowCompactResponse showCompactions() throws TException {
    throw new UnsupportedOperationException("showCompactions is not supported");
  }

  public void addDynamicPartitions(long txnId, String dbName, String tblName, List<String> partNames)
    throws TException {
    throw new UnsupportedOperationException("addDynamicPartitions is not supported");
  }

  public void addDynamicPartitions(
      long txnId,
      String dbName,
      String tblName,
      List<String> partNames,
      DataOperationType operationType)
    throws TException {
    throw new UnsupportedOperationException("addDynamicPartitions is not supported");
  }

  public void insertTable(org.apache.hadoop.hive.metastore.api.Table table, boolean overwrite) throws MetaException {
    throw new UnsupportedOperationException("insertTable is not supported");
  }

  public NotificationEventResponse getNextNotification(
      long lastEventId,
      int maxEvents,
      IMetaStoreClient.NotificationFilter notificationFilter)
    throws TException {
    throw new UnsupportedOperationException("getNextNotification is not supported");
  }

  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    throw new UnsupportedOperationException("getCurrentNotificationEventId is not supported");
  }

  public FireEventResponse fireListenerEvent(FireEventRequest fireEventRequest) throws TException {
    throw new UnsupportedOperationException("fireListenerEvent is not supported");
  }

  public ShowLocksResponse showLocks() throws TException {
    throw new UnsupportedOperationException("showLocks is not supported");
  }

  public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
    throw new UnsupportedOperationException("showLocks is not supported");
  }

  public GetOpenTxnsInfoResponse showTxns() throws TException {
    throw new UnsupportedOperationException("showTxns is not supported");
  }

  public void unlock(long lockId) throws TException {
    throw new UnsupportedOperationException("unlock is not supported");
  }

  public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
    throw new UnsupportedOperationException("getFileMetadata is not supported");
  }

  public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
      List<Long> fileIds,
      ByteBuffer sarg,
      boolean doGetFooters)
    throws TException {
    throw new UnsupportedOperationException("getFileMetadataBySarg is not supported");
  }

  public void clearFileMetadata(List<Long> fileIds) throws TException {
    throw new UnsupportedOperationException("clearFileMetadata is not supported");
  }

  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
    throw new UnsupportedOperationException("putFileMetadata is not supported");
  }

  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request) throws TException {
    throw new UnsupportedOperationException("setPartitionColumnStatistics is not supported");
  }

  public boolean cacheFileMetadata(String dbName, String tblName, String partName, boolean allParts) throws TException {
    throw new UnsupportedOperationException("cacheFileMetadata is not supported");
  }

  public int addPartitionsSpecProxy(PartitionSpecProxy pSpec) throws TException {
    throw new UnsupportedOperationException("addPartitionsSpecProxy is unsupported");
  }
}
