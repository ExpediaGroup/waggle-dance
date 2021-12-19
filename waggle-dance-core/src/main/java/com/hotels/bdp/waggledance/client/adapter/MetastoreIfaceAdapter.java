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
package com.hotels.bdp.waggledance.client.adapter;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;

import com.facebook.fb303.fb_status;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;

/**
 * Adapter to wrap a {@link ThriftHiveMetastore.Iface} in a {@link IMetaStoreClient}.
 */
public class MetastoreIfaceAdapter implements CloseableThriftHiveMetastoreIface {

  private final IMetaStoreClient client;

  public MetastoreIfaceAdapter(IMetaStoreClient client) {
    this.client = client;
  }

  @Override
  public String getMetaConf(String key) throws MetaException, TException {
    return client.getMetaConf(key);
  }

  @Override
  public void setMetaConf(String key, String value) throws MetaException, TException {
    client.setMetaConf(key, value);
  }

  @Override
  public void create_database(Database database)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    client.createDatabase(database);
  }

  @Override
  public Database get_database(String name) throws NoSuchObjectException, MetaException, TException {
    return client.getDatabase(name);
  }

  @Override
  public void drop_database(String name, boolean deleteData, boolean cascade)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new InvalidOperationException();
  }

  @Override
  public List<String> get_databases(String pattern) throws MetaException, TException {
    return client.getDatabases(pattern);
  }

  @Override
  public List<String> get_all_databases() throws MetaException, TException {
    return client.getAllDatabases();
  }

  @Override
  public void alter_database(String dbname, Database db) throws MetaException, NoSuchObjectException, TException {
    client.alterDatabase(dbname, db);
  }

  @Override
  public Type get_type(String name) throws MetaException, NoSuchObjectException, TException {
    throw new TException("Unsupported via federation using Waggle Dance");
  }

  @Override
  public boolean create_type(Type type)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    throw new TException("Unsupported via federation using Waggle Dance");
  }

  @Override
  public boolean drop_type(String type) throws MetaException, NoSuchObjectException, TException {
    throw new TException("Unsupported via federation using Waggle Dance");
  }

  @Override
  public Map<String, Type> get_type_all(String name) throws MetaException, TException {
    throw new TException("Unsupported via federation using Waggle Dance");
  }

  @Override
  public List<FieldSchema> get_fields(String db_name, String table_name)
    throws MetaException, UnknownTableException, UnknownDBException, TException {
    return client.getFields(db_name, table_name);
  }

  @Override
  public List<FieldSchema> get_fields_with_environment_context(
      String db_name,
      String table_name,
      EnvironmentContext environment_context)
    throws MetaException, UnknownTableException, UnknownDBException, TException {
    return client.getFields(db_name, table_name);
  }

  @Override
  public List<FieldSchema> get_schema(String db_name, String table_name)
    throws MetaException, UnknownTableException, UnknownDBException, TException {
    return client.getSchema(db_name, table_name);
  }

  @Override
  public List<FieldSchema> get_schema_with_environment_context(
      String db_name,
      String table_name,
      EnvironmentContext environment_context)
    throws MetaException, UnknownTableException, UnknownDBException, TException {
    return client.getSchema(db_name, table_name);
  }

  @Override
  public void create_table(Table tbl)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    client.createTable(tbl);
  }

  @Override
  public void create_table_with_environment_context(Table tbl, EnvironmentContext environment_context)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    client.createTable(tbl);
  }

  @Override
  public void create_table_with_constraints(Table tbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    client.createTableWithConstraints(tbl, primaryKeys, foreignKeys);
  }

  @Override
  public void drop_constraint(DropConstraintRequest req) throws NoSuchObjectException, MetaException, TException {
    client.dropConstraint(req.getDbname(), req.getTablename(), req.getConstraintname());
  }

  @Override
  public void add_primary_key(AddPrimaryKeyRequest req) throws NoSuchObjectException, MetaException, TException {
    client.addPrimaryKey(req.getPrimaryKeyCols());
  }

  @Override
  public void add_foreign_key(AddForeignKeyRequest req) throws NoSuchObjectException, MetaException, TException {
    client.addForeignKey(req.getForeignKeyCols());
  }

  @Override
  public void drop_table(String dbname, String name, boolean deleteData)
    throws NoSuchObjectException, MetaException, TException {
    client.dropTable(dbname, name, deleteData, false);
  }

  @Override
  public void drop_table_with_environment_context(
      String dbname,
      String name,
      boolean deleteData,
      EnvironmentContext environment_context)
    throws NoSuchObjectException, MetaException, TException {
    client.dropTable(dbname, name, deleteData, false);
  }

  @Override
  public List<String> get_tables(String db_name, String pattern) throws MetaException, TException {
    return client.getTables(db_name, pattern);
  }

  @Override
  public List<String> get_tables_by_type(String db_name, String pattern, String tableType)
    throws MetaException, TException {
    return client.getTables(db_name, pattern, TableType.valueOf(tableType));
  }

  @Override
  public List<TableMeta> get_table_meta(String db_patterns, String tbl_patterns, List<String> tbl_types)
    throws MetaException, TException {
    return client.getTableMeta(db_patterns, tbl_patterns, tbl_types);
  }

  @Override
  public List<String> get_all_tables(String db_name) throws MetaException, TException {
    return client.getAllTables(db_name);
  }

  @Override
  public Table get_table(String dbname, String tbl_name) throws MetaException, NoSuchObjectException, TException {
    return client.getTable(dbname, tbl_name);
  }

  @Override
  public List<Table> get_table_objects_by_name(String dbname, List<String> tbl_names) throws TException {
    return client.getTableObjectsByName(dbname, tbl_names);
  }

  @Override
  public GetTableResult get_table_req(GetTableRequest req) throws MetaException, NoSuchObjectException, TException {
    return new GetTableResult(client.getTable(req.getDbName(), req.getTblName()));
  }

  @Override
  public GetTablesResult get_table_objects_by_name_req(GetTablesRequest req)
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return new GetTablesResult(client.getTableObjectsByName(req.getDbName(), req.getTblNames()));
  }

  @Override
  public List<String> get_table_names_by_filter(String dbname, String filter, short max_tables)
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return client.listTableNamesByFilter(dbname, filter, max_tables);
  }

  @Override
  public void alter_table(String dbname, String tbl_name, Table new_tbl)
    throws InvalidOperationException, MetaException, TException {
    client.alter_table(dbname, tbl_name, new_tbl);

  }

  @Override
  public void alter_table_with_environment_context(
      String dbname,
      String tbl_name,
      Table new_tbl,
      EnvironmentContext environment_context)
    throws InvalidOperationException, MetaException, TException {
    client.alter_table_with_environmentContext(dbname, tbl_name, new_tbl, environment_context);
  }

  @Override
  public void alter_table_with_cascade(String dbname, String tbl_name, Table new_tbl, boolean cascade)
    throws InvalidOperationException, MetaException, TException {
    EnvironmentContext context = new EnvironmentContext();
    context.putToProperties(StatsSetupConst.CASCADE, Boolean.toString(cascade));
    alter_table_with_environment_context(dbname, tbl_name, new_tbl, context);
  }

  @Override
  public Partition add_partition(Partition new_part)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return client.add_partition(new_part);
  }

  @Override
  public Partition add_partition_with_environment_context(Partition new_part, EnvironmentContext environment_context)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return client.add_partition(new_part);
  }

  @Override
  public int add_partitions(List<Partition> new_parts)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return client.add_partitions(new_parts);
  }

  @Override
  public int add_partitions_pspec(List<PartitionSpec> new_parts)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    PartitionSpecProxy spec = PartitionSpecProxy.Factory.get(new_parts);
    return client.add_partitions_pspec(spec);
  }

  @Override
  public Partition append_partition(String db_name, String tbl_name, List<String> part_vals)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return client.appendPartition(db_name, db_name, part_vals);
  }

  @Override
  public AddPartitionsResult add_partitions_req(AddPartitionsRequest request)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    List<Partition> add_partitions = client
        .add_partitions(request.getParts(), request.isIfNotExists(), request.isNeedResult());
    AddPartitionsResult result = new AddPartitionsResult();
    result.setPartitions(add_partitions);
    return result;
  }

  @Override
  public Partition append_partition_with_environment_context(
      String db_name,
      String tbl_name,
      List<String> part_vals,
      EnvironmentContext environment_context)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return client.appendPartition(db_name, tbl_name, part_vals);
  }

  @Override
  public Partition append_partition_by_name(String db_name, String tbl_name, String part_name)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return client.appendPartition(db_name, tbl_name, part_name);
  }

  @Override
  public Partition append_partition_by_name_with_environment_context(
      String db_name,
      String tbl_name,
      String part_name,
      EnvironmentContext environment_context)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return client.appendPartition(db_name, tbl_name, part_name);
  }

  @Override
  public boolean drop_partition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData)
    throws NoSuchObjectException, MetaException, TException {
    return client.dropPartition(db_name, tbl_name, part_vals, deleteData);
  }

  @Override
  public boolean drop_partition_with_environment_context(
      String db_name,
      String tbl_name,
      List<String> part_vals,
      boolean deleteData,
      EnvironmentContext environment_context)
    throws NoSuchObjectException, MetaException, TException {
    return client.dropPartition(db_name, tbl_name, part_vals, deleteData);
  }

  @Override
  public boolean drop_partition_by_name(String db_name, String tbl_name, String part_name, boolean deleteData)
    throws NoSuchObjectException, MetaException, TException {
    return client.dropPartition(db_name, tbl_name, part_name, deleteData);
  }

  @Override
  public boolean drop_partition_by_name_with_environment_context(
      String db_name,
      String tbl_name,
      String part_name,
      boolean deleteData,
      EnvironmentContext environment_context)
    throws NoSuchObjectException, MetaException, TException {
    return client.dropPartition(db_name, tbl_name, part_name, deleteData);
  }

  @Override
  public DropPartitionsResult drop_partitions_req(DropPartitionsRequest req)
    throws NoSuchObjectException, MetaException, TException {
    List<ObjectPair<Integer, byte[]>> partitionExpressions = req
        .getParts()
        .getExprs()
        .stream()
        .map(k -> new ObjectPair<>(k.getPartArchiveLevel(), k.getExpr()))
        .collect(Collectors.toList());
    List<Partition> dropPartitions = client
        .dropPartitions(req.getDbName(), req.getTblName(), partitionExpressions, new PartitionDropOptions());
    DropPartitionsResult result = new DropPartitionsResult();
    result.setPartitions(dropPartitions);
    return result;
  }

  @Override
  public Partition get_partition(String db_name, String tbl_name, List<String> part_vals)
    throws MetaException, NoSuchObjectException, TException {
    return client.getPartition(db_name, tbl_name, part_vals);
  }

  @Override
  public Partition exchange_partition(
      Map<String, String> partitionSpecs,
      String source_db,
      String source_table_name,
      String dest_db,
      String dest_table_name)
    throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    return client.exchange_partition(partitionSpecs, source_db, source_table_name, dest_db, dest_table_name);
  }

  @Override
  public List<Partition> exchange_partitions(
      Map<String, String> partitionSpecs,
      String source_db,
      String source_table_name,
      String dest_db,
      String dest_table_name)
    throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    return client.exchange_partitions(partitionSpecs, source_db, source_table_name, dest_db, dest_table_name);
  }

  @Override
  public Partition get_partition_with_auth(
      String db_name,
      String tbl_name,
      List<String> part_vals,
      String user_name,
      List<String> group_names)
    throws MetaException, NoSuchObjectException, TException {
    return client.getPartitionWithAuthInfo(db_name, tbl_name, part_vals, user_name, group_names);
  }

  @Override
  public Partition get_partition_by_name(String db_name, String tbl_name, String part_name)
    throws MetaException, NoSuchObjectException, TException {
    return client.getPartition(db_name, tbl_name, part_name);
  }

  @Override
  public List<Partition> get_partitions(String db_name, String tbl_name, short max_parts)
    throws NoSuchObjectException, MetaException, TException {
    return client.listPartitions(db_name, tbl_name, max_parts);
  }

  @Override
  public List<Partition> get_partitions_with_auth(
      String db_name,
      String tbl_name,
      short max_parts,
      String user_name,
      List<String> group_names)
    throws NoSuchObjectException, MetaException, TException {
    return client.listPartitionsWithAuthInfo(db_name, tbl_name, max_parts, user_name, group_names);
  }

  @Override
  public List<PartitionSpec> get_partitions_pspec(String db_name, String tbl_name, int max_parts)
    throws NoSuchObjectException, MetaException, TException {
    return client.listPartitionSpecs(db_name, tbl_name, max_parts).toPartitionSpec();
  }

  @Override
  public List<String> get_partition_names(String db_name, String tbl_name, short max_parts)
    throws MetaException, TException {
    return client.listPartitionNames(db_name, tbl_name, max_parts);
  }

  @Override
  public PartitionValuesResponse get_partition_values(PartitionValuesRequest request)
    throws MetaException, NoSuchObjectException, TException {
    return client.listPartitionValues(request);
  }

  @Override
  public List<Partition> get_partitions_ps(String db_name, String tbl_name, List<String> part_vals, short max_parts)
    throws MetaException, NoSuchObjectException, TException {
    return client.listPartitions(db_name, tbl_name, part_vals, max_parts);
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
    return client.listPartitionsWithAuthInfo(db_name, tbl_name, part_vals, max_parts, user_name, group_names);
  }

  @Override
  public List<String> get_partition_names_ps(String db_name, String tbl_name, List<String> part_vals, short max_parts)
    throws MetaException, NoSuchObjectException, TException {
    return client.listPartitionNames(db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<Partition> get_partitions_by_filter(String db_name, String tbl_name, String filter, short max_parts)
    throws MetaException, NoSuchObjectException, TException {
    return client.listPartitionsByFilter(db_name, tbl_name, filter, max_parts);
  }

  @Override
  public List<PartitionSpec> get_part_specs_by_filter(String db_name, String tbl_name, String filter, int max_parts)
    throws MetaException, NoSuchObjectException, TException {
    return client.listPartitionSpecsByFilter(db_name, tbl_name, filter, max_parts).toPartitionSpec();
  }

  @Override
  public PartitionsByExprResult get_partitions_by_expr(PartitionsByExprRequest req)
    throws MetaException, NoSuchObjectException, TException {
    List<Partition> result = new ArrayList<>();
    boolean hasUnknownPartitions = client
        .listPartitionsByExpr(req.getDbName(), req.getTblName(), req.getExpr(), req.getDefaultPartitionName(),
            req.getMaxParts(), result);
    return new PartitionsByExprResult(result, hasUnknownPartitions);
  }

  @Override
  public int get_num_partitions_by_filter(String db_name, String tbl_name, String filter)
    throws MetaException, NoSuchObjectException, TException {
    return client.getNumPartitionsByFilter(db_name, tbl_name, filter);
  }

  @Override
  public List<Partition> get_partitions_by_names(String db_name, String tbl_name, List<String> names)
    throws MetaException, NoSuchObjectException, TException {
    return client.getPartitionsByNames(db_name, tbl_name, names);
  }

  @Override
  public void alter_partition(String db_name, String tbl_name, Partition new_part)
    throws InvalidOperationException, MetaException, TException {
    client.alter_partition(db_name, tbl_name, new_part);
  }

  @Override
  public void alter_partitions(String db_name, String tbl_name, List<Partition> new_parts)
    throws InvalidOperationException, MetaException, TException {
    client.alter_partitions(db_name, tbl_name, new_parts);
  }

  @Override
  public void alter_partitions_with_environment_context(
      String db_name,
      String tbl_name,
      List<Partition> new_parts,
      EnvironmentContext environment_context)
    throws InvalidOperationException, MetaException, TException {
    client.alter_partitions(db_name, tbl_name, new_parts, environment_context);
  }

  @Override
  public void alter_partition_with_environment_context(
      String db_name,
      String tbl_name,
      Partition new_part,
      EnvironmentContext environment_context)
    throws InvalidOperationException, MetaException, TException {
    client.alter_partition(db_name, tbl_name, new_part, environment_context);
  }

  @Override
  public void rename_partition(String db_name, String tbl_name, List<String> part_vals, Partition new_part)
    throws InvalidOperationException, MetaException, TException {
    client.renamePartition(db_name, tbl_name, part_vals, new_part);
  }

  @Override
  public boolean partition_name_has_valid_characters(List<String> part_vals, boolean throw_exception)
    throws MetaException, TException {
    try {
      client.validatePartitionNameCharacters(part_vals);
      return true;
    } catch (MetaException e) {
      if (throw_exception) {
        throw e;
      }
      return false;
    }
  }

  @Override
  public String get_config_value(String name, String defaultValue) throws ConfigValSecurityException, TException {
    return client.getConfigValue(name, defaultValue);
  }

  @Override
  public List<String> partition_name_to_vals(String part_name) throws MetaException, TException {
    return client.partitionNameToVals(part_name);
  }

  @Override
  public Map<String, String> partition_name_to_spec(String part_name) throws MetaException, TException {
    return client.partitionNameToSpec(part_name);
  }

  @Override
  public void markPartitionForEvent(
      String db_name,
      String tbl_name,
      Map<String, String> part_vals,
      PartitionEventType eventType)
    throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException, UnknownPartitionException,
    InvalidPartitionException, TException {
    client.markPartitionForEvent(db_name, tbl_name, part_vals, eventType);
  }

  @Override
  public boolean isPartitionMarkedForEvent(
      String db_name,
      String tbl_name,
      Map<String, String> part_vals,
      PartitionEventType eventType)
    throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException, UnknownPartitionException,
    InvalidPartitionException, TException {
    return client.isPartitionMarkedForEvent(db_name, tbl_name, part_vals, eventType);
  }

  @Override
  public Index add_index(Index new_index, Table index_table)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    client.createIndex(new_index, index_table);
    return new_index;
  }

  @Override
  public void alter_index(String dbname, String base_tbl_name, String idx_name, Index new_idx)
    throws InvalidOperationException, MetaException, TException {
    client.alter_index(dbname, base_tbl_name, idx_name, new_idx);
  }

  @Override
  public boolean drop_index_by_name(String db_name, String tbl_name, String index_name, boolean deleteData)
    throws NoSuchObjectException, MetaException, TException {
    return client.dropIndex(db_name, tbl_name, index_name, deleteData);
  }

  @Override
  public Index get_index_by_name(String db_name, String tbl_name, String index_name)
    throws MetaException, NoSuchObjectException, TException {
    return client.getIndex(db_name, tbl_name, index_name);
  }

  @Override
  public List<Index> get_indexes(String db_name, String tbl_name, short max_indexes)
    throws NoSuchObjectException, MetaException, TException {
    return client.listIndexes(db_name, tbl_name, max_indexes);
  }

  @Override
  public List<String> get_index_names(String db_name, String tbl_name, short max_indexes)
    throws MetaException, TException {
    return client.listIndexNames(db_name, tbl_name, max_indexes);
  }

  @Override
  public PrimaryKeysResponse get_primary_keys(PrimaryKeysRequest request)
    throws MetaException, NoSuchObjectException, TException {
    return new PrimaryKeysResponse(client.getPrimaryKeys(request));
  }

  @Override
  public ForeignKeysResponse get_foreign_keys(ForeignKeysRequest request)
    throws MetaException, NoSuchObjectException, TException {
    return new ForeignKeysResponse(client.getForeignKeys(request));
  }

  @Override
  public boolean update_table_column_statistics(ColumnStatistics stats_obj)
    throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    return client.updateTableColumnStatistics(stats_obj);
  }

  @Override
  public boolean update_partition_column_statistics(ColumnStatistics stats_obj)
    throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    return client.updatePartitionColumnStatistics(stats_obj);
  }

  @Override
  public ColumnStatistics get_table_column_statistics(String db_name, String tbl_name, String col_name)
    throws NoSuchObjectException, MetaException, InvalidInputException, InvalidObjectException, TException {
    // Hard to implement and not supported in Glue currently.
    throw new UnsupportedOperationException("getTableColumnStatistics is not supported");
  }

  @Override
  public ColumnStatistics get_partition_column_statistics(
      String db_name,
      String tbl_name,
      String part_name,
      String col_name)
    throws NoSuchObjectException, MetaException, InvalidInputException, InvalidObjectException, TException {
    // Hard to implement and not supported in Glue currently.
    throw new UnsupportedOperationException("getPartitionColumnStatistics is not supported");
  }

  @Override
  public TableStatsResult get_table_statistics_req(TableStatsRequest request)
    throws NoSuchObjectException, MetaException, TException {
    return new TableStatsResult(
        client.getTableColumnStatistics(request.getDbName(), request.getTblName(), request.getColNames()));
  }

  @Override
  public PartitionsStatsResult get_partitions_statistics_req(PartitionsStatsRequest request)
    throws NoSuchObjectException, MetaException, TException {
    return new PartitionsStatsResult(client
        .getPartitionColumnStatistics(request.getDbName(), request.getTblName(), request.getPartNames(),
            request.getColNames()));
  }

  @Override
  public AggrStats get_aggr_stats_for(PartitionsStatsRequest request)
    throws NoSuchObjectException, MetaException, TException {
    return client
        .getAggrColStatsFor(request.getDbName(), request.getTblName(), request.getColNames(), request.getPartNames());
  }

  @Override
  public boolean set_aggr_stats_for(SetPartitionsStatsRequest request)
    throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    return client.setPartitionColumnStatistics(request);
  }

  @Override
  public boolean delete_partition_column_statistics(String db_name, String tbl_name, String part_name, String col_name)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException, TException {
    return client.deletePartitionColumnStatistics(db_name, tbl_name, part_name, col_name);
  }

  @Override
  public boolean delete_table_column_statistics(String db_name, String tbl_name, String col_name)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException, TException {
    return client.deleteTableColumnStatistics(db_name, tbl_name, col_name);
  }

  @Override
  public void create_function(Function func)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    client.createFunction(func);
  }

  @Override
  public void drop_function(String dbName, String funcName) throws NoSuchObjectException, MetaException, TException {
    client.dropFunction(dbName, funcName);
  }

  @Override
  public void alter_function(String dbName, String funcName, Function newFunc)
    throws InvalidOperationException, MetaException, TException {
    client.alterFunction(dbName, funcName, newFunc);
  }

  @Override
  public List<String> get_functions(String dbName, String pattern) throws MetaException, TException {
    return client.getFunctions(dbName, pattern);
  }

  @Override
  public Function get_function(String dbName, String funcName) throws MetaException, NoSuchObjectException, TException {
    return client.getFunction(dbName, funcName);
  }

  @Override
  public GetAllFunctionsResponse get_all_functions() throws MetaException, TException {
    return client.getAllFunctions();
  }

  @Override
  public boolean create_role(Role role) throws MetaException, TException {
    return client.create_role(role);
  }

  @Override
  public boolean drop_role(String role_name) throws MetaException, TException {
    return client.drop_role(role_name);
  }

  @Override
  public List<String> get_role_names() throws MetaException, TException {
    return client.listRoleNames();
  }

  @Override
  public boolean grant_role(
      String role_name,
      String principal_name,
      PrincipalType principal_type,
      String grantor,
      PrincipalType grantorType,
      boolean grant_option)
    throws MetaException, TException {
    return client.grant_role(role_name, principal_name, principal_type, grantor, grantorType, grant_option);
  }

  @Override
  public boolean revoke_role(String role_name, String principal_name, PrincipalType principal_type)
    throws MetaException, TException {
    return client.revoke_role(role_name, principal_name, principal_type, false);
  }

  @Override
  public List<Role> list_roles(String principal_name, PrincipalType principal_type) throws MetaException, TException {
    return client.list_roles(principal_name, principal_type);
  }

  @Override
  public GrantRevokeRoleResponse grant_revoke_role(GrantRevokeRoleRequest request) throws MetaException, TException {
    throw new UnsupportedOperationException("grant_revoke_role is not supported");
  }

  @Override
  public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest request)
    throws MetaException, TException {
    return client.get_principals_in_role(request);
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(GetRoleGrantsForPrincipalRequest request)
    throws MetaException, TException {
    return client.get_role_grants_for_principal(request);
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String user_name, List<String> group_names)
    throws MetaException, TException {
    return client.get_privilege_set(hiveObject, user_name, group_names);
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(
      String principal_name,
      PrincipalType principal_type,
      HiveObjectRef hiveObject)
    throws MetaException, TException {
    return client.list_privileges(principal_name, principal_type, hiveObject);
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privileges) throws MetaException, TException {
    return client.grant_privileges(privileges);
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privileges) throws MetaException, TException {
    return client.revoke_privileges(privileges, false);
  }

  @Override
  public GrantRevokePrivilegeResponse grant_revoke_privileges(GrantRevokePrivilegeRequest request)
    throws MetaException, TException {
    throw new UnsupportedOperationException("grant_revoke_privileges is not supported");
  }

  @Override
  public List<String> set_ugi(String user_name, List<String> group_names) throws MetaException, TException {
    return Arrays.asList();
  }

  @Override
  public String get_delegation_token(String token_owner, String renewer_kerberos_principal_name)
    throws MetaException, TException {
    return client.getDelegationToken(token_owner, renewer_kerberos_principal_name);
  }

  @Override
  public long renew_delegation_token(String token_str_form) throws MetaException, TException {
    return client.renewDelegationToken(token_str_form);
  }

  @Override
  public void cancel_delegation_token(String token_str_form) throws MetaException, TException {
    client.cancelDelegationToken(token_str_form);
  }

  @Override
  public boolean add_token(String token_identifier, String delegation_token) throws TException {
    return client.addToken(token_identifier, delegation_token);
  }

  @Override
  public boolean remove_token(String token_identifier) throws TException {
    return client.removeToken(token_identifier);
  }

  @Override
  public String get_token(String token_identifier) throws TException {
    return client.getToken(token_identifier);
  }

  @Override
  public List<String> get_all_token_identifiers() throws TException {
    return client.getAllTokenIdentifiers();
  }

  @Override
  public int add_master_key(String key) throws MetaException, TException {
    return client.addMasterKey(key);
  }

  @Override
  public void update_master_key(int seq_number, String key) throws NoSuchObjectException, MetaException, TException {
    client.updateMasterKey(seq_number, key);
  }

  @Override
  public boolean remove_master_key(int key_seq) throws TException {
    return client.removeMasterKey(key_seq);
  }

  @Override
  public List<String> get_master_keys() throws TException {
    return Arrays.asList(client.getMasterKeys());
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
    client.abortTxns(Arrays.asList(rqst.getTxnid()));

  }

  @Override
  public void abort_txns(AbortTxnsRequest rqst) throws NoSuchTxnException, TException {
    client.abortTxns(rqst.getTxn_ids());
  }

  @Override
  public void commit_txn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException, TException {
    client.commitTxn(rqst.getTxnid());
  }

  @Override
  public LockResponse lock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException, TException {
    return client.lock(rqst);
  }

  @Override
  public LockResponse check_lock(CheckLockRequest rqst)
    throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
    return client.checkLock(rqst.getLockid());
  }

  @Override
  public void unlock(UnlockRequest rqst) throws NoSuchLockException, TxnOpenException, TException {
    client.unlock(rqst.getLockid());
  }

  @Override
  public ShowLocksResponse show_locks(ShowLocksRequest rqst) throws TException {
    return client.showLocks(rqst);
  }

  @Override
  public void heartbeat(HeartbeatRequest ids)
    throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
    client.heartbeat(ids.getTxnid(), ids.getLockid());
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeat_txn_range(HeartbeatTxnRangeRequest txns) throws TException {
    return client.heartbeatTxnRange(txns.getMin(), txns.getMax());
  }

  @Override
  public void compact(CompactionRequest rqst) throws TException {
    client
        .compact2(rqst.getDbname(), rqst.getTablename(), rqst.getPartitionname(), rqst.getType(), rqst.getProperties());
  }

  @Override
  public CompactionResponse compact2(CompactionRequest rqst) throws TException {
    return client
        .compact2(rqst.getDbname(), rqst.getTablename(), rqst.getPartitionname(), rqst.getType(), rqst.getProperties());
  }

  @Override
  public ShowCompactResponse show_compact(ShowCompactRequest rqst) throws TException {
    return client.showCompactions();
  }

  @Override
  public void add_dynamic_partitions(AddDynamicPartitions rqst)
    throws NoSuchTxnException, TxnAbortedException, TException {
    client
        .addDynamicPartitions(rqst.getTxnid(), rqst.getDbname(), rqst.getTablename(), rqst.getPartitionnames(),
            rqst.getOperationType());
  }

  @Override
  public NotificationEventResponse get_next_notification(NotificationEventRequest rqst) throws TException {
    return client.getNextNotification(rqst.getLastEvent(), rqst.getMaxEvents(), null);
  }

  @Override
  public CurrentNotificationEventId get_current_notificationEventId() throws TException {
    return client.getCurrentNotificationEventId();
  }

  @Override
  public FireEventResponse fire_listener_event(FireEventRequest rqst) throws TException {
    return client.fireListenerEvent(rqst);
  }

  @Override
  public void flushCache() throws TException {
    client.flushCache();
  }

  @Override
  public GetFileMetadataByExprResult get_file_metadata_by_expr(GetFileMetadataByExprRequest req) throws TException {
    Iterable<Entry<Long, MetadataPpdResult>> fileMetadataBySarg = client
        .getFileMetadataBySarg(req.getFileIds(), req.bufferForExpr(), req.isDoGetFooters());
    Map<Long, MetadataPpdResult> result = new HashMap<>();
    for (Entry<Long, MetadataPpdResult> entry : fileMetadataBySarg) {
      result.put(entry.getKey(), entry.getValue());
    }
    boolean isSupported = false;
    return new GetFileMetadataByExprResult(result, isSupported);
  }

  @Override
  public GetFileMetadataResult get_file_metadata(GetFileMetadataRequest req) throws TException {
    Iterable<Entry<Long, ByteBuffer>> fileMetadata = client.getFileMetadata(req.getFileIds());
    Map<Long, ByteBuffer> result = new HashMap<>();
    for (Entry<Long, ByteBuffer> entry : fileMetadata) {
      result.put(entry.getKey(), entry.getValue());
    }
    boolean isSupported = false;
    return new GetFileMetadataResult(result, isSupported);
  }

  @Override
  public PutFileMetadataResult put_file_metadata(PutFileMetadataRequest req) throws TException {
    client.putFileMetadata(req.getFileIds(), req.getMetadata());
    return new PutFileMetadataResult();
  }

  @Override
  public ClearFileMetadataResult clear_file_metadata(ClearFileMetadataRequest req) throws TException {
    client.clearFileMetadata(req.getFileIds());
    return new ClearFileMetadataResult();
  }

  @Override
  public CacheFileMetadataResult cache_file_metadata(CacheFileMetadataRequest req) throws TException {
    client.cacheFileMetadata(req.getDbName(), req.getTblName(), req.getPartName(), req.isIsAllParts());
    return new CacheFileMetadataResult(false);
  }

  @Override
  public long aliveSince() throws TException {
    return 0;
  }

  @Override
  public long getCounter(String arg0) throws TException {
    return 0;
  }

  @Override
  public Map<String, Long> getCounters() throws TException {
    return Collections.emptyMap();
  }

  @Override
  public String getCpuProfile(int arg0) throws TException {
    return "";
  }

  @Override
  public String getName() throws TException {
    return "waggle-dance-iMetastore-client-adapter";
  }

  @Override
  public String getOption(String arg0) throws TException {
    return "";
  }

  @Override
  public Map<String, String> getOptions() throws TException {
    return Collections.emptyMap();
  }

  @Override
  public fb_status getStatus() throws TException {
    return fb_status.ALIVE;
  }

  @Override
  public String getStatusDetails() throws TException {
    return fb_status.ALIVE.toString();
  }

  @Override
  public String getVersion() throws TException {
    return "";
  }

  @Override
  public void reinitialize() throws TException {
    client.reconnect();
  }

  @Override
  public void setOption(String arg0, String arg1) throws TException {

  }

  @Override
  public void shutdown() throws TException {
    client.close();
  }

  @Override
  public void close() throws IOException {
    client.close();
  }

  @Override
  public boolean isOpen() {
    return true;
  }

}
