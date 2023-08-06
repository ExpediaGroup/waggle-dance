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
package com.hotels.bdp.waggledance.mapping.model;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.AddCheckConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDefaultConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AddForeignKeyRequest;
import org.apache.hadoop.hive.metastore.api.AddNotNullConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AddUniqueConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.AlterISchemaRequest;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.DropConstraintRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.GrantRevokePrivilegeRequest;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.MapSchemaVersionToSerdeRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.SetSchemaVersionStateRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.thrift.TException;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.mapping.service.GrammarUtils;

@AllArgsConstructor
@Log4j2
public class DatabaseMappingImpl implements DatabaseMapping {

  private final MetaStoreMapping metaStoreMapping;
  private final QueryMapping queryMapping;

  @Override
  public MetaStoreFilterHook getMetastoreFilter() {
    return metaStoreMapping.getMetastoreFilter();
  }

  @Override
  public Table transformOutboundTable(Table table) {
    String originalDatabaseName = table.getDbName();
    String databaseName = metaStoreMapping.transformOutboundDatabaseName(originalDatabaseName);
    table.setDbName(databaseName);
    if (databaseName.equalsIgnoreCase(originalDatabaseName)) {
      // Skip all the view parsing if nothing is going to change, the parsing is not without problems and we can't catch
      // all use cases here. For instance Presto creates views that are stored in these fields and this is stored
      // differently than Hive. There might be others.
      return table;
    }
    if (table.isSetViewExpandedText()) {
      try {
        log.debug("Transforming ViewExpandedText: {}", table.getViewExpandedText());
        table
            .setViewExpandedText(
                queryMapping.transformOutboundDatabaseName(metaStoreMapping, table.getViewExpandedText()));
      } catch (WaggleDanceException e) {
        log.debug("Error while transforming databaseName in ViewExpandedText, keeping original", e);
      }
    }
    if (table.isSetViewOriginalText()) {
      try {
        log.debug("Transforming ViewOriginalText: {}", table.getViewOriginalText());
        table
            .setViewOriginalText(
                queryMapping.transformOutboundDatabaseName(metaStoreMapping, table.getViewOriginalText()));
      } catch (WaggleDanceException e) {
        // We are hitting a bug in hive (https://issues.apache.org/jira/browse/HIVE-19896) that prevents the
        // ViewOriginalText to be parsed, if we leave the ViewOriginalText we'll have the wrong database names in it so
        // we set the ViewExpandedText to at least return a "correct" view query string. Hard to see what is the usage
        // and impact of this.
        log.debug("Error while transforming databaseName in ViewOriginalText, using ViewExpandedText if available", e);
        if (table.isSetViewExpandedText()) {
          table.setViewOriginalText(table.getViewExpandedText());
        }
      }
    }
    return table;
  }

  @Override
  public Partition transformOutboundPartition(Partition partition) {
    partition.setDbName(metaStoreMapping.transformOutboundDatabaseName(partition.getDbName()));
    return partition;
  }

  @Override
  public ISchema transformOutboundISchema(ISchema iSchema) {
    iSchema.setDbName(metaStoreMapping.transformOutboundDatabaseName(iSchema.getDbName()));
    return iSchema;
  }

  @Override
  public Table transformInboundTable(Table table) {
    table.setDbName(metaStoreMapping.transformInboundDatabaseName(table.getDbName()));
    return table;
  }

  @Override
  public Partition transformInboundPartition(Partition partition) {
    partition.setDbName(metaStoreMapping.transformInboundDatabaseName(partition.getDbName()));
    return partition;
  }

  @Override
  public ISchema transformInboundISchema(ISchema iSchema) {
    iSchema.setDbName(metaStoreMapping.transformInboundDatabaseName(iSchema.getDbName()));
    return iSchema;
  }

  @Override
  public Function transformOutboundFunction(Function function) {
    function.setDbName(metaStoreMapping.transformOutboundDatabaseName(function.getDbName()));
    return function;
  }

  @Override
  public HiveObjectRef transformInboundHiveObjectRef(HiveObjectRef obj) {
    obj.setDbName(metaStoreMapping.transformInboundDatabaseName(obj.getDbName()));
    if (obj.getObjectType() == HiveObjectType.DATABASE) {
      obj.setObjectName(metaStoreMapping.transformInboundDatabaseName(obj.getObjectName()));
    }
    return obj;
  }

  @Override
  public HiveObjectRef transformOutboundHiveObjectRef(HiveObjectRef obj) {
    obj.setDbName(metaStoreMapping.transformOutboundDatabaseName(obj.getDbName()));
    if (obj.getObjectType() == HiveObjectType.DATABASE) {
      obj.setObjectName(metaStoreMapping.transformOutboundDatabaseName(obj.getObjectName()));
    }
    return obj;
  }

  @Override
  public PartitionSpec transformOutboundPartitionSpec(PartitionSpec partitionSpec) {
    partitionSpec.setDbName(metaStoreMapping.transformOutboundDatabaseName(partitionSpec.getDbName()));
    return partitionSpec;
  }

  @Override
  public PartitionsStatsRequest transformInboundPartitionsStatsRequest(PartitionsStatsRequest request) {
    request.setDbName(metaStoreMapping.transformInboundDatabaseName(request.getDbName()));
    return request;
  }

  @Override
  public TableStatsRequest transformInboundTableStatsRequest(TableStatsRequest request) {
    request.setDbName(metaStoreMapping.transformInboundDatabaseName(request.getDbName()));
    return request;
  }

  @Override
  public PartitionsByExprRequest transformInboundPartitionsByExprRequest(PartitionsByExprRequest req) {
    req.setDbName(metaStoreMapping.transformInboundDatabaseName(req.getDbName()));
    return req;
  }

  @Override
  public PartitionsByExprResult transformOutboundPartitionsByExprResult(PartitionsByExprResult result) {
    result.setPartitions(transformOutboundPartitions(result.getPartitions()));
    return result;
  }

  @Override
  public Iface getClient() {
    return metaStoreMapping.getClient();
  }

  @Override
  public String transformOutboundDatabaseName(String databaseName) {
    databaseName = GrammarUtils.removeCatName(databaseName);
    return metaStoreMapping.transformOutboundDatabaseName(databaseName);
  }

  @Override
  public List<String> transformOutboundDatabaseNameMultiple(String databaseName) {
    databaseName = GrammarUtils.removeCatName(databaseName);
    return metaStoreMapping.transformOutboundDatabaseNameMultiple(databaseName);
  }

  @Override
  public Database transformOutboundDatabase(Database database) {
    return metaStoreMapping.transformOutboundDatabase(database);
  }

  @Override
  public String transformInboundDatabaseName(String databaseName) {
    databaseName = GrammarUtils.removeCatName(databaseName);
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
    req.setDbName(metaStoreMapping.transformInboundDatabaseName(req.getDbName()));
    return req;
  }

  @Override
  public FireEventRequest transformInboundFireEventRequest(FireEventRequest rqst) {
    rqst.setDbName(metaStoreMapping.transformInboundDatabaseName(rqst.getDbName()));
    return rqst;
  }

  @Override
  public ForeignKeysRequest transformInboundForeignKeysRequest(ForeignKeysRequest request) {
    String parentDbName = request.getParent_db_name() == null ? null
        : metaStoreMapping.transformInboundDatabaseName(request.getParent_db_name());
    String foreignDbName = request.getForeign_db_name() == null ? null
        : metaStoreMapping.transformInboundDatabaseName(request.getForeign_db_name());

    request.setParent_db_name(parentDbName);
    request.setForeign_db_name(foreignDbName);
    return request;
  }

  @Override
  public ForeignKeysResponse transformOutboundForeignKeysResponse(ForeignKeysResponse response) {
    for (SQLForeignKey key : response.getForeignKeys()) {
      key.setPktable_db(metaStoreMapping.transformOutboundDatabaseName(key.getPktable_db()));
      key.setFktable_db(metaStoreMapping.transformOutboundDatabaseName(key.getFktable_db()));
    }
    return response;
  }

  @Override
  public PrimaryKeysRequest transformInboundPrimaryKeysRequest(PrimaryKeysRequest request) {
    request.setDb_name(metaStoreMapping.transformInboundDatabaseName(request.getDb_name()));
    return request;
  }

  @Override
  public PrimaryKeysResponse transformOutboundPrimaryKeysResponse(PrimaryKeysResponse response) {
    for (SQLPrimaryKey key : response.getPrimaryKeys()) {
      key.setTable_db(metaStoreMapping.transformOutboundDatabaseName(key.getTable_db()));
    }
    return response;
  }

  @Override
  public TableMeta transformOutboundTableMeta(TableMeta tableMeta) {
    tableMeta.setDbName(metaStoreMapping.transformOutboundDatabaseName(tableMeta.getDbName()));
    return tableMeta;
  }

  @Override
  public AddDynamicPartitions transformInboundAddDynamicPartitions(AddDynamicPartitions request) {
    request.setDbname(metaStoreMapping.transformInboundDatabaseName(request.getDbname()));
    return request;
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
    databaseName = GrammarUtils.removeCatName(databaseName);
    return metaStoreMapping.checkWritePermissions(transformInboundDatabaseName(databaseName));
  }

  @Override
  public DropConstraintRequest transformInboundDropConstraintRequest(DropConstraintRequest request) {
    request.setDbname(metaStoreMapping.transformInboundDatabaseName(request.getDbname()));
    return request;
  }

  @Override
  public AddPartitionsRequest transformInboundAddPartitionsRequest(AddPartitionsRequest request) {
    request.setDbName(metaStoreMapping.transformInboundDatabaseName(request.getDbName()));
    request.setParts(transformInboundPartitions(request.getParts()));
    return request;
  }

  @Override
  public AddPartitionsResult transformOutboundAddPartitionsResult(AddPartitionsResult result) {
    result.setPartitions(transformOutboundPartitions(result.getPartitions()));
    return result;
  }

  @Override
  public DropPartitionsRequest transformInboundDropPartitionRequest(DropPartitionsRequest request) {
    request.setDbName(metaStoreMapping.transformInboundDatabaseName(request.getDbName()));
    return request;
  }

  @Override
  public DropPartitionsResult transformOutboundDropPartitionsResult(DropPartitionsResult result) {
    result.setPartitions(transformOutboundPartitions(result.getPartitions()));
    return result;
  }

  @Override
  public List<Partition> transformOutboundPartitions(List<Partition> partitions) {
    for (Partition partition : partitions) {
      transformOutboundPartition(partition);
    }
    return partitions;
  }

  @Override
  public List<PartitionSpec> transformOutboundPartitionSpecs(List<PartitionSpec> partitionSpecs) {
    for (PartitionSpec partitionSpec : partitionSpecs) {
      transformOutboundPartitionSpec(partitionSpec);
    }
    return partitionSpecs;
  }

  @Override
  public List<Partition> transformInboundPartitions(List<Partition> partitions) {
    for (Partition partition : partitions) {
      transformInboundPartition(partition);
    }
    return partitions;
  }

  @Override
  public List<ISchema> transformOutboundISchemas(List<ISchema> iSchemaList) {
    for (ISchema iSchema : iSchemaList) {
      transformOutboundISchema(iSchema);
    }
    return iSchemaList;
  }

  @Override
  public ColumnStatistics transformInboundColumnStatistics(ColumnStatistics columnStatistics) {
    columnStatistics
        .getStatsDesc()
        .setDbName(metaStoreMapping.transformInboundDatabaseName(columnStatistics.getStatsDesc().getDbName()));
    return columnStatistics;
  }

  @Override
  public ColumnStatistics transformOutboundColumnStatistics(ColumnStatistics columnStatistics) {
    columnStatistics
        .getStatsDesc()
        .setDbName(metaStoreMapping.transformOutboundDatabaseName(columnStatistics.getStatsDesc().getDbName()));
    return columnStatistics;
  }

  @Override
  public SetPartitionsStatsRequest transformInboundSetPartitionStatsRequest(SetPartitionsStatsRequest request) {
    if (request.isSetColStats()) {
      for (ColumnStatistics stats : request.getColStats()) {
        transformInboundColumnStatistics(stats);
      }
    }
    return request;
  }

  @Override
  public Function transformInboundFunction(Function function) {
    function.setDbName(metaStoreMapping.transformInboundDatabaseName(function.getDbName()));
    return function;
  }

  @Override
  public List<HiveObjectPrivilege> transformOutboundHiveObjectPrivileges(List<HiveObjectPrivilege> privileges) {
    for (HiveObjectPrivilege privilege : privileges) {
      privilege.setHiveObject(transformOutboundHiveObjectRef(privilege.getHiveObject()));
    }
    return privileges;
  }

  @Override
  public PrivilegeBag transformInboundPrivilegeBag(PrivilegeBag privilegeBag) {
    if (privilegeBag.isSetPrivileges()) {
      for (HiveObjectPrivilege privilege : privilegeBag.getPrivileges()) {
        privilege.setHiveObject(transformInboundHiveObjectRef(privilege.getHiveObject()));
      }
    }
    return privilegeBag;
  }

  @Override
  public GrantRevokePrivilegeRequest transformInboundGrantRevokePrivilegesRequest(GrantRevokePrivilegeRequest request) {
    if (request.isSetPrivileges()) {
      request.setPrivileges(transformInboundPrivilegeBag(request.getPrivileges()));
    }
    return request;
  }

  @Override
  public LockRequest transformInboundLockRequest(LockRequest request) {
    if (request.isSetComponent()) {
      for (LockComponent component : request.getComponent()) {
        component.setDbname(metaStoreMapping.transformInboundDatabaseName(component.getDbname()));
      }
    }
    return request;
  }

  @Override
  public CompactionRequest transformInboundCompactionRequest(CompactionRequest request) {
    request.setDbname(metaStoreMapping.transformInboundDatabaseName(request.getDbname()));
    return request;
  }

  @Override
  public void createDatabase(Database database)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    metaStoreMapping.createDatabase(database);
  }

  @Override
  public Database transformInboundDatabase(Database database) {
    database.setName(metaStoreMapping.transformInboundDatabaseName(database.getName()));
    return database;
  }

  @Override
  public List<PartitionSpec> transformInboundPartitionSpecs(List<PartitionSpec> partitionSpecs) {
    for (PartitionSpec partitionSpec : partitionSpecs) {
      partitionSpec.setDbName(metaStoreMapping.transformInboundDatabaseName(partitionSpec.getDbName()));
    }
    return partitionSpecs;
  }

  @Override
  public GetTableRequest transformInboundGetTableRequest(GetTableRequest request) {
    request.setDbName(metaStoreMapping.transformInboundDatabaseName(request.getDbName()));
    return request;
  }

  @Override
  public GetTableResult transformOutboundGetTableResult(GetTableResult result) {
    transformOutboundTable(result.getTable());
    return result;
  }

  @Override
  public GetTablesRequest transformInboundGetTablesRequest(GetTablesRequest request) {
    request.setDbName(metaStoreMapping.transformInboundDatabaseName(request.getDbName()));
    return request;
  }

  @Override
  public GetTablesResult transformOutboundGetTablesResult(GetTablesResult result) {
    for (Table table : result.getTables()) {
      transformOutboundTable(table);
    }
    return result;
  }

  @Override
  public PartitionValuesRequest transformInboundPartitionValuesRequest(PartitionValuesRequest request) {
    request.setDbName(transformInboundDatabaseName(request.getDbName()));
    return request;
  }

  @Override
  public List<SQLPrimaryKey> transformInboundSQLPrimaryKeys(List<SQLPrimaryKey> sqlPrimaryKeys) {
    for(SQLPrimaryKey sqlPrimaryKey: sqlPrimaryKeys) {
      sqlPrimaryKey.setTable_db(transformInboundDatabaseName(sqlPrimaryKey.getTable_db()));
    }
    return sqlPrimaryKeys;
  }

  @Override
  public List<SQLForeignKey> transformInboundSQLForeignKeys(List<SQLForeignKey> sqlForeignKeys) {
    for(SQLForeignKey sqlForeignKey: sqlForeignKeys) {
      sqlForeignKey.setPktable_db(transformInboundDatabaseName(sqlForeignKey.getPktable_db()));
      sqlForeignKey.setFktable_db(transformInboundDatabaseName(sqlForeignKey.getFktable_db()));
    }
    return sqlForeignKeys;
  }

  @Override
  public List<SQLUniqueConstraint> transformInboundSQLUniqueConstraints(List<SQLUniqueConstraint> sqlUniqueConstraints) {
    for(SQLUniqueConstraint sqlUniqueConstraint: sqlUniqueConstraints) {
      sqlUniqueConstraint.setTable_db(transformInboundDatabaseName(sqlUniqueConstraint.getTable_db()));
    }
    return sqlUniqueConstraints;
  }

  @Override
  public List<SQLNotNullConstraint> transformInboundSQLNotNullConstraints(List<SQLNotNullConstraint> sqlNotNullConstraints) {
    for(SQLNotNullConstraint sqlNotNullConstraint: sqlNotNullConstraints) {
      sqlNotNullConstraint.setTable_db(transformInboundDatabaseName(sqlNotNullConstraint.getTable_db()));
    }
    return sqlNotNullConstraints;
  }

  @Override
  public List<SQLDefaultConstraint> transformInboundSQLDefaultConstraints(List<SQLDefaultConstraint> sqlDefaultConstraints) {
    for(SQLDefaultConstraint sqlDefaultConstraint: sqlDefaultConstraints) {
      sqlDefaultConstraint.setTable_db(transformInboundDatabaseName(sqlDefaultConstraint.getTable_db()));
    }
    return sqlDefaultConstraints;
  }

  @Override
  public List<SQLCheckConstraint> transformInboundSQLCheckConstraints(List<SQLCheckConstraint> sqlCheckConstraints) {
    for(SQLCheckConstraint sqlCheckConstraint: sqlCheckConstraints) {
      sqlCheckConstraint.setTable_db(transformInboundDatabaseName(sqlCheckConstraint.getTable_db()));
    }
    return sqlCheckConstraints;
  }


  @Override
  public ReplTblWriteIdStateRequest transformInboundReplTblWriteIdStateRequest(ReplTblWriteIdStateRequest request) {
    request.setDbName(transformInboundDatabaseName(request.getDbName()));
    return request;
  }


  @Override
  public AllocateTableWriteIdsRequest transformInboundAllocateTableWriteIdsRequest(AllocateTableWriteIdsRequest request) {
    request.setDbName(transformInboundDatabaseName(request.getDbName()));
    return request;
  }


  @Override
  public AlterISchemaRequest transformInboundAlterISchemaRequest(AlterISchemaRequest request) {
    if(request.getName() !=null) {
      request.setName(transformInboundISchemaName(request.getName()));
    }
    if(request.getNewSchema() != null) {
      request.setNewSchema(transformInboundISchema(request.getNewSchema()));
    }
    return request;
  }


  @Override
  public SchemaVersion transformInboundSchemaVersion(SchemaVersion schemaVersion) {
    if(schemaVersion.getSchema() !=null ) {
      schemaVersion.getSchema().setDbName(transformInboundDatabaseName(schemaVersion.getSchema().getDbName()));
    }
    return schemaVersion;
  }


  @Override
  public SchemaVersion transformOutboundSchemaVersion(SchemaVersion schemaVersion) {
    if(schemaVersion.getSchema() !=null ) {
      schemaVersion.getSchema().setDbName(metaStoreMapping.transformOutboundDatabaseName(
              schemaVersion.getSchema().getDbName()));
    }
    return schemaVersion;
  }

  @Override
  public List<SchemaVersion> transformOutboundSchemaVersions(List<SchemaVersion> schemaVersions) {
    for(SchemaVersion schemaVersion: schemaVersions) {
      transformOutboundSchemaVersion(schemaVersion);
    }
    return schemaVersions;
  }

  @Override
  public ISchemaName transformInboundISchemaName(ISchemaName iSchemaName) {
    iSchemaName.setDbName(transformInboundDatabaseName(iSchemaName.getDbName()));
    return iSchemaName;
  }

  @Override
  public ISchemaName transformOutboundISchemaName(ISchemaName iSchemaName) {
    iSchemaName.setDbName(transformOutboundDatabaseName(iSchemaName.getDbName()));
    return iSchemaName;
  }

  @Override
  public AddForeignKeyRequest transformInboundAddForeignKeyRequest(AddForeignKeyRequest request) {
    for(SQLForeignKey sqlForeignKey: request.getForeignKeyCols()) {
      sqlForeignKey.setPktable_db(transformInboundDatabaseName(sqlForeignKey.getPktable_db()));
      sqlForeignKey.setFktable_db(transformInboundDatabaseName(sqlForeignKey.getFktable_db()));
    }
    return request;
  }


  @Override
  public AddUniqueConstraintRequest transformInboundAddUniqueConstraintRequest(AddUniqueConstraintRequest request) {
    for(SQLUniqueConstraint sqlUniqueConstraint: request.getUniqueConstraintCols()) {
      sqlUniqueConstraint.setTable_db(transformInboundDatabaseName(sqlUniqueConstraint.getTable_db()));
    }
    return request;
  }


  @Override
  public AddNotNullConstraintRequest transformInboundAddNotNullConstraintRequest(AddNotNullConstraintRequest request) {
    for(SQLNotNullConstraint sqlNotNullConstraint: request.getNotNullConstraintCols()) {
      sqlNotNullConstraint.setTable_db(transformInboundDatabaseName(sqlNotNullConstraint.getTable_db()));
    }
    return request;
  }


  @Override
  public AddDefaultConstraintRequest transformInboundAddDefaultConstraintRequest(AddDefaultConstraintRequest request) {
    for(SQLDefaultConstraint sqlDefaultConstraint: request.getDefaultConstraintCols()) {
      sqlDefaultConstraint.setTable_db(transformInboundDatabaseName(sqlDefaultConstraint.getTable_db()));
    }
    return request;
  }


  @Override
  public AddCheckConstraintRequest transformInboundAddCheckConstraintRequest(AddCheckConstraintRequest request) {
    for(SQLCheckConstraint sqlCheckConstraint: request.getCheckConstraintCols()) {
      sqlCheckConstraint.setTable_db(transformInboundDatabaseName(sqlCheckConstraint.getTable_db()));
    }
    return request;
  }


  @Override
  public FindSchemasByColsResp transformOutboundFindSchemasByColsResp(FindSchemasByColsResp response) {
    for(SchemaVersionDescriptor schemaVersionDescriptor: response.getSchemaVersions()) {
      if(schemaVersionDescriptor.getSchema() != null) {
        schemaVersionDescriptor.setSchema(transformOutboundISchemaName(schemaVersionDescriptor.getSchema()));
      }
    }
    return response;
  }


  @Override
  public SchemaVersionDescriptor transformInboundSchemaVersionDescriptor(SchemaVersionDescriptor request) {
    if(request.getSchema() !=null) {
      request.getSchema().setDbName(transformInboundDatabaseName(request.getSchema().getDbName()));
    }
    return request;
  }


  @Override
  public MapSchemaVersionToSerdeRequest transformInboundMapSchemaVersionToSerdeRequest(MapSchemaVersionToSerdeRequest request) {
    if(request.getSchemaVersion() != null && request.getSchemaVersion().getSchema() !=null) {
      request.getSchemaVersion().getSchema().setDbName(transformInboundDatabaseName(
              request.getSchemaVersion().getSchema().getDbName()));
    }
    return request;
  }


  @Override
  public SetSchemaVersionStateRequest transformInboundSetSchemaVersionStateRequest(SetSchemaVersionStateRequest request) {
    if(request.getSchemaVersion() != null && request.getSchemaVersion().getSchema() !=null) {
      request.getSchemaVersion().getSchema().setDbName(transformInboundDatabaseName(
              request.getSchemaVersion().getSchema().getDbName()));
    }
    return request;
  }


  @Override
  public NotificationEventsCountRequest transformInboundNotificationEventsCountRequest(NotificationEventsCountRequest request) {
    request.setDbName(transformInboundDatabaseName(request.getDbName()));
    return request;
  }


  @Override
  public UniqueConstraintsRequest transformInboundUniqueConstraintsRequest(UniqueConstraintsRequest request) {
    request.setDb_name(transformInboundDatabaseName(request.getDb_name()));
    return request;
  }

  @Override
  public UniqueConstraintsResponse transformOutboundUniqueConstraintsResponse(UniqueConstraintsResponse response) {
    for(SQLUniqueConstraint sqlUniqueConstraint: response.getUniqueConstraints()) {
      sqlUniqueConstraint.setTable_db(transformOutboundDatabaseName(sqlUniqueConstraint.getTable_db()));
    }
    return response;
  }


  @Override
  public NotNullConstraintsRequest transformInboundNotNullConstraintsRequest(NotNullConstraintsRequest request) {
    request.setDb_name(transformInboundDatabaseName(request.getDb_name()));
    return request;
  }

  @Override
  public NotNullConstraintsResponse transformOutboundNotNullConstraintsResponse(NotNullConstraintsResponse response) {
    for(SQLNotNullConstraint sqlNotNullConstraint: response.getNotNullConstraints()) {
      sqlNotNullConstraint.setTable_db(transformOutboundDatabaseName(sqlNotNullConstraint.getTable_db()));
    }
    return response;
  }


  @Override
  public DefaultConstraintsRequest transformInboundDefaultConstraintsRequest(DefaultConstraintsRequest request) {
    request.setDb_name(transformInboundDatabaseName(request.getDb_name()));
    return request;
  }

  @Override
  public DefaultConstraintsResponse transformOutboundDefaultConstraintsResponse(DefaultConstraintsResponse response) {
    for(SQLDefaultConstraint sqlDefaultConstraint: response.getDefaultConstraints()) {
      sqlDefaultConstraint.setTable_db(transformOutboundDatabaseName(sqlDefaultConstraint.getTable_db()));
    }
    return response;
  }


  @Override
  public CheckConstraintsRequest transformInboundCheckConstraintsRequest(CheckConstraintsRequest request) {
    request.setDb_name(transformInboundDatabaseName(request.getDb_name()));
    return request;
  }

  @Override
  public CheckConstraintsResponse transformOutboundCheckConstraintsResponse(CheckConstraintsResponse response) {
    for(SQLCheckConstraint sqlCheckConstraint: response.getCheckConstraints()) {
      sqlCheckConstraint.setTable_db(transformOutboundDatabaseName(sqlCheckConstraint.getTable_db()));
    }
    return response;
  }


  @Override
  public CreationMetadata transformInboundCreationMetadata(CreationMetadata request) {
    request.setDbName(transformInboundDatabaseName(request.getDbName()));
    return request;
  }


  @Override
  public long getLatency() {
    return metaStoreMapping.getLatency();
  }

}
