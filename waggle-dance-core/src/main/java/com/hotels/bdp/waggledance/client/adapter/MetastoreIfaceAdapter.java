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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.facebook.fb303.fb_status;
import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;

/**
 * Adapter to wrap a {@link ThriftHiveMetastore.Iface} in a {@link IMetaStoreClient}.
 */
public class MetastoreIfaceAdapter implements CloseableThriftHiveMetastoreIface {

  private final static Logger log = LoggerFactory.getLogger(MetastoreIfaceAdapter.class);

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
    // TODO Auto-generated method stub

  }

  @Override
  public void create_database(Database database)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public Database get_database(String name) throws NoSuchObjectException, MetaException, TException {
    return client.getDatabase(name);
  }

  @Override
  public void drop_database(String name, boolean deleteData, boolean cascade)
    throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public List<String> get_databases(String pattern) throws MetaException, TException {
    return client.getDatabases(pattern);
  }

  @Override
  public List<String> get_all_databases() throws MetaException, TException {
    log.info("get_all_databases GLUE");
    return client.getAllDatabases();
  }

  @Override
  public void alter_database(String dbname, Database db) throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public Type get_type(String name) throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean create_type(Type type)
    throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean drop_type(String type) throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Map<String, Type> get_type_all(String name) throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<FieldSchema> get_fields(String db_name, String table_name)
    throws MetaException, UnknownTableException, UnknownDBException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<FieldSchema> get_fields_with_environment_context(
      String db_name,
      String table_name,
      EnvironmentContext environment_context)
    throws MetaException, UnknownTableException, UnknownDBException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<FieldSchema> get_schema(String db_name, String table_name)
    throws MetaException, UnknownTableException, UnknownDBException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<FieldSchema> get_schema_with_environment_context(
      String db_name,
      String table_name,
      EnvironmentContext environment_context)
    throws MetaException, UnknownTableException, UnknownDBException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void create_table(Table tbl)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void create_table_with_environment_context(Table tbl, EnvironmentContext environment_context)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void create_table_with_constraints(Table tbl, List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void drop_constraint(DropConstraintRequest req) throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void add_primary_key(AddPrimaryKeyRequest req) throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void add_foreign_key(AddForeignKeyRequest req) throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void drop_table(String dbname, String name, boolean deleteData)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void drop_table_with_environment_context(
      String dbname,
      String name,
      boolean deleteData,
      EnvironmentContext environment_context)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public List<String> get_tables(String db_name, String pattern) throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> get_tables_by_type(String db_name, String pattern, String tableType)
    throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<TableMeta> get_table_meta(String db_patterns, String tbl_patterns, List<String> tbl_types)
    throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> get_all_tables(String db_name) throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Table get_table(String dbname, String tbl_name) throws MetaException, NoSuchObjectException, TException {
    return client.getTable(dbname, tbl_name);
  }

  @Override
  public List<Table> get_table_objects_by_name(String dbname, List<String> tbl_names) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetTableResult get_table_req(GetTableRequest req) throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetTablesResult get_table_objects_by_name_req(GetTablesRequest req)
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> get_table_names_by_filter(String dbname, String filter, short max_tables)
    throws MetaException, InvalidOperationException, UnknownDBException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void alter_table(String dbname, String tbl_name, Table new_tbl)
    throws InvalidOperationException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void alter_table_with_environment_context(
      String dbname,
      String tbl_name,
      Table new_tbl,
      EnvironmentContext environment_context)
    throws InvalidOperationException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void alter_table_with_cascade(String dbname, String tbl_name, Table new_tbl, boolean cascade)
    throws InvalidOperationException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public Partition add_partition(Partition new_part)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Partition add_partition_with_environment_context(Partition new_part, EnvironmentContext environment_context)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int add_partitions(List<Partition> new_parts)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int add_partitions_pspec(List<PartitionSpec> new_parts)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Partition append_partition(String db_name, String tbl_name, List<String> part_vals)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AddPartitionsResult add_partitions_req(AddPartitionsRequest request)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Partition append_partition_with_environment_context(
      String db_name,
      String tbl_name,
      List<String> part_vals,
      EnvironmentContext environment_context)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Partition append_partition_by_name(String db_name, String tbl_name, String part_name)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Partition append_partition_by_name_with_environment_context(
      String db_name,
      String tbl_name,
      String part_name,
      EnvironmentContext environment_context)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean drop_partition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean drop_partition_with_environment_context(
      String db_name,
      String tbl_name,
      List<String> part_vals,
      boolean deleteData,
      EnvironmentContext environment_context)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean drop_partition_by_name(String db_name, String tbl_name, String part_name, boolean deleteData)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean drop_partition_by_name_with_environment_context(
      String db_name,
      String tbl_name,
      String part_name,
      boolean deleteData,
      EnvironmentContext environment_context)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public DropPartitionsResult drop_partitions_req(DropPartitionsRequest req)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Partition get_partition(String db_name, String tbl_name, List<String> part_vals)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Partition exchange_partition(
      Map<String, String> partitionSpecs,
      String source_db,
      String source_table_name,
      String dest_db,
      String dest_table_name)
    throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Partition> exchange_partitions(
      Map<String, String> partitionSpecs,
      String source_db,
      String source_table_name,
      String dest_db,
      String dest_table_name)
    throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Partition get_partition_with_auth(
      String db_name,
      String tbl_name,
      List<String> part_vals,
      String user_name,
      List<String> group_names)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Partition get_partition_by_name(String db_name, String tbl_name, String part_name)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Partition> get_partitions(String db_name, String tbl_name, short max_parts)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Partition> get_partitions_with_auth(
      String db_name,
      String tbl_name,
      short max_parts,
      String user_name,
      List<String> group_names)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<PartitionSpec> get_partitions_pspec(String db_name, String tbl_name, int max_parts)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> get_partition_names(String db_name, String tbl_name, short max_parts)
    throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PartitionValuesResponse get_partition_values(PartitionValuesRequest request)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Partition> get_partitions_ps(String db_name, String tbl_name, List<String> part_vals, short max_parts)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> get_partition_names_ps(String db_name, String tbl_name, List<String> part_vals, short max_parts)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Partition> get_partitions_by_filter(String db_name, String tbl_name, String filter, short max_parts)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<PartitionSpec> get_part_specs_by_filter(String db_name, String tbl_name, String filter, int max_parts)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PartitionsByExprResult get_partitions_by_expr(PartitionsByExprRequest req)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int get_num_partitions_by_filter(String db_name, String tbl_name, String filter)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public List<Partition> get_partitions_by_names(String db_name, String tbl_name, List<String> names)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void alter_partition(String db_name, String tbl_name, Partition new_part)
    throws InvalidOperationException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void alter_partitions(String db_name, String tbl_name, List<Partition> new_parts)
    throws InvalidOperationException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void alter_partitions_with_environment_context(
      String db_name,
      String tbl_name,
      List<Partition> new_parts,
      EnvironmentContext environment_context)
    throws InvalidOperationException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void alter_partition_with_environment_context(
      String db_name,
      String tbl_name,
      Partition new_part,
      EnvironmentContext environment_context)
    throws InvalidOperationException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void rename_partition(String db_name, String tbl_name, List<String> part_vals, Partition new_part)
    throws InvalidOperationException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean partition_name_has_valid_characters(List<String> part_vals, boolean throw_exception)
    throws MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String get_config_value(String name, String defaultValue) throws ConfigValSecurityException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> partition_name_to_vals(String part_name) throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<String, String> partition_name_to_spec(String part_name) throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void markPartitionForEvent(
      String db_name,
      String tbl_name,
      Map<String, String> part_vals,
      PartitionEventType eventType)
    throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException, UnknownPartitionException,
    InvalidPartitionException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isPartitionMarkedForEvent(
      String db_name,
      String tbl_name,
      Map<String, String> part_vals,
      PartitionEventType eventType)
    throws MetaException, NoSuchObjectException, UnknownDBException, UnknownTableException, UnknownPartitionException,
    InvalidPartitionException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Index add_index(Index new_index, Table index_table)
    throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void alter_index(String dbname, String base_tbl_name, String idx_name, Index new_idx)
    throws InvalidOperationException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean drop_index_by_name(String db_name, String tbl_name, String index_name, boolean deleteData)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Index get_index_by_name(String db_name, String tbl_name, String index_name)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<Index> get_indexes(String db_name, String tbl_name, short max_indexes)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> get_index_names(String db_name, String tbl_name, short max_indexes)
    throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PrimaryKeysResponse get_primary_keys(PrimaryKeysRequest request)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ForeignKeysResponse get_foreign_keys(ForeignKeysRequest request)
    throws MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean update_table_column_statistics(ColumnStatistics stats_obj)
    throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean update_partition_column_statistics(ColumnStatistics stats_obj)
    throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public ColumnStatistics get_table_column_statistics(String db_name, String tbl_name, String col_name)
    throws NoSuchObjectException, MetaException, InvalidInputException, InvalidObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ColumnStatistics get_partition_column_statistics(
      String db_name,
      String tbl_name,
      String part_name,
      String col_name)
    throws NoSuchObjectException, MetaException, InvalidInputException, InvalidObjectException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TableStatsResult get_table_statistics_req(TableStatsRequest request)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PartitionsStatsResult get_partitions_statistics_req(PartitionsStatsRequest request)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public AggrStats get_aggr_stats_for(PartitionsStatsRequest request)
    throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean set_aggr_stats_for(SetPartitionsStatsRequest request)
    throws NoSuchObjectException, InvalidObjectException, MetaException, InvalidInputException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean delete_partition_column_statistics(String db_name, String tbl_name, String part_name, String col_name)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean delete_table_column_statistics(String db_name, String tbl_name, String col_name)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void create_function(Function func)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void drop_function(String dbName, String funcName) throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void alter_function(String dbName, String funcName, Function newFunc)
    throws InvalidOperationException, MetaException, TException {
    // TODO Auto-generated method stub

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
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean drop_role(String role_name) throws MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<String> get_role_names() throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
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
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean revoke_role(String role_name, String principal_name, PrincipalType principal_type)
    throws MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<Role> list_roles(String principal_name, PrincipalType principal_type) throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GrantRevokeRoleResponse grant_revoke_role(GrantRevokeRoleRequest request) throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest request)
    throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(GetRoleGrantsForPrincipalRequest request)
    throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String user_name, List<String> group_names)
    throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(
      String principal_name,
      PrincipalType principal_type,
      HiveObjectRef hiveObject)
    throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privileges) throws MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privileges) throws MetaException, TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public GrantRevokePrivilegeResponse grant_revoke_privileges(GrantRevokePrivilegeRequest request)
    throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> set_ugi(String user_name, List<String> group_names) throws MetaException, TException {
    return Lists.newArrayList();
  }

  @Override
  public String get_delegation_token(String token_owner, String renewer_kerberos_principal_name)
    throws MetaException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long renew_delegation_token(String token_str_form) throws MetaException, TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void cancel_delegation_token(String token_str_form) throws MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean add_token(String token_identifier, String delegation_token) throws TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean remove_token(String token_identifier) throws TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String get_token(String token_identifier) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> get_all_token_identifiers() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int add_master_key(String key) throws MetaException, TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void update_master_key(int seq_number, String key) throws NoSuchObjectException, MetaException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean remove_master_key(int key_seq) throws TException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public List<String> get_master_keys() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetOpenTxnsResponse get_open_txns() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetOpenTxnsInfoResponse get_open_txns_info() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public OpenTxnsResponse open_txns(OpenTxnRequest rqst) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void abort_txn(AbortTxnRequest rqst) throws NoSuchTxnException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void abort_txns(AbortTxnsRequest rqst) throws NoSuchTxnException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void commit_txn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public LockResponse lock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public LockResponse check_lock(CheckLockRequest rqst)
    throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void unlock(UnlockRequest rqst) throws NoSuchLockException, TxnOpenException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public ShowLocksResponse show_locks(ShowLocksRequest rqst) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void heartbeat(HeartbeatRequest ids)
    throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public HeartbeatTxnRangeResponse heartbeat_txn_range(HeartbeatTxnRangeRequest txns) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void compact(CompactionRequest rqst) throws TException {
    // TODO Auto-generated method stub

  }

  @Override
  public CompactionResponse compact2(CompactionRequest rqst) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ShowCompactResponse show_compact(ShowCompactRequest rqst) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void add_dynamic_partitions(AddDynamicPartitions rqst)
    throws NoSuchTxnException, TxnAbortedException, TException {
    // TODO Auto-generated method stub

  }

  @Override
  public NotificationEventResponse get_next_notification(NotificationEventRequest rqst) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CurrentNotificationEventId get_current_notificationEventId() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public FireEventResponse fire_listener_event(FireEventRequest rqst) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void flushCache() throws TException {
    // TODO Auto-generated method stub

  }

  @Override
  public GetFileMetadataByExprResult get_file_metadata_by_expr(GetFileMetadataByExprRequest req) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GetFileMetadataResult get_file_metadata(GetFileMetadataRequest req) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PutFileMetadataResult put_file_metadata(PutFileMetadataRequest req) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ClearFileMetadataResult clear_file_metadata(ClearFileMetadataRequest req) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CacheFileMetadataResult cache_file_metadata(CacheFileMetadataRequest req) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long aliveSince() throws TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public long getCounter(String arg0) throws TException {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Map<String, Long> getCounters() throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getCpuProfile(int arg0) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getName() throws TException {
    return "waggle-dance-client";
  }

  @Override
  public String getOption(String arg0) throws TException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<String, String> getOptions() throws TException {
    // TODO Auto-generated method stub
    return null;
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
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void reinitialize() throws TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setOption(String arg0, String arg1) throws TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void shutdown() throws TException {
    // TODO Auto-generated method stub

  }

  @Override
  public void close() throws IOException {
    // TODO Auto-generated method stub

  }

  @Override
  public boolean isOpen() {
    return true;
  }

}
