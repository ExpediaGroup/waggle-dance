/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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
import java.lang.reflect.Constructor;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AddPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.AddPartitionsResult;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CacheFileMetadataRequest;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropConstraintRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsRequest;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
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
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.LockComponent;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprRequest;
import org.apache.hadoop.hive.metastore.api.PartitionsByExprResult;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.security.krb5.RealmException;

import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.server.WaggleDanceServerException;

public class DatabaseMappingImpl implements DatabaseMapping {

  private final static Logger log = LoggerFactory.getLogger(DatabaseMappingImpl.class);

  private final MetaStoreMapping metaStoreMapping;
  private final QueryMapping queryMapping;
  private final MetaStoreFilterHook metastoreFilter;

  public DatabaseMappingImpl(MetaStoreMapping metaStoreMapping, QueryMapping queryMapping) {
    this.metaStoreMapping = metaStoreMapping;
    this.queryMapping = queryMapping;
    this.metastoreFilter = loadMetastoreFilter();
  }

  @Override
  public MetaStoreFilterHook getMetastoreFilter() {
    return metastoreFilter;
  }

  public MetaStoreFilterHook loadMetastoreFilter() {
    HiveConf conf = new HiveConf();
    conf.set(HiveConf.ConfVars.METASTORE_FILTER_HOOK.varname, "com.hotels.bdp.waggledance.mapping.model.AlluxioMetastoreFilter");
    Class<? extends MetaStoreFilterHook> authProviderClass = conf.getClass(
        HiveConf.ConfVars.METASTORE_FILTER_HOOK.varname, DefaultMetaStoreFilterHookImpl.class, MetaStoreFilterHook.class);
    String msg = "Unable to create instance of " + authProviderClass.getName() + ": ";
    try {
      System.out.println("-----------------" + authProviderClass.toString());
      Constructor<? extends MetaStoreFilterHook> constructor =
          authProviderClass.getConstructor(HiveConf.class);
      System.out.println("-----------------");
      return constructor.newInstance(conf);
    } catch (Exception e) {
      throw new WaggleDanceServerException(msg + e);
    }
  }

  @Override
  public Table transformOutboundTable(Table table) {
    table.setDbName(metaStoreMapping.transformOutboundDatabaseName(table.getDbName()));
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
  public Index transformOutboundIndex(Index index) {
    index.setDbName(metaStoreMapping.transformOutboundDatabaseName(index.getDbName()));
    return index;
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
  public Index transformInboundIndex(Index index) {
    index.setDbName(metaStoreMapping.transformInboundDatabaseName(index.getDbName()));
    return index;
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
    return metaStoreMapping.transformOutboundDatabaseName(databaseName);
  }

  @Override
  public List<String> transformOutboundDatabaseNameMultiple(String databaseName) {
    return metaStoreMapping.transformOutboundDatabaseNameMultiple(databaseName);
  }

  @Override
  public Database transformOutboundDatabase(Database database) {
    return metaStoreMapping.transformOutboundDatabase(database);
  }

  @Override
  public String transformInboundDatabaseName(String databaseName) {
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
  public List<Index> transformOutboundIndexes(List<Index> indexes) {
    for (Index index : indexes) {
      transformOutboundIndex(index);
    }
    return indexes;
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
  public long getLatency() {
    return metaStoreMapping.getLatency();
  }

}
