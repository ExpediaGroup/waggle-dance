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
package com.hotels.bdp.waggledance.server;

import java.lang.reflect.*;
import java.util.Set;

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import com.hotels.bdp.waggledance.server.security.ranger.RangerAccessControlHandler;

public class RangerWrappingHMSHandler implements InvocationHandler {
  private final static Logger LOG = LoggerFactory.getLogger(RangerWrappingHMSHandler.class);

  private final IHMSHandler baseHandler;
  private final RangerAccessControlHandler rangerAccessControlHandler;
  private String user;
  private Set<String> groups;

  public static IHMSHandler newProxyInstance(IHMSHandler baseHandler,
                                             RangerAccessControlHandler rangerAccessControlHandler) {
    return (IHMSHandler) Proxy.newProxyInstance(RangerWrappingHMSHandler.class.getClassLoader(),
            new Class[]{IHMSHandler.class}, new RangerWrappingHMSHandler(baseHandler, rangerAccessControlHandler));
  }

  public RangerWrappingHMSHandler(IHMSHandler baseHandler, RangerAccessControlHandler rangerAccessControlHandler) {
    this.baseHandler = baseHandler;
    this.rangerAccessControlHandler = rangerAccessControlHandler;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    try {
      if (TokenWrappingHMSHandler.getToken().isEmpty())
        return method.invoke(baseHandler, args);

      if (user == null) {
        String userName = UserGroupInformation.getCurrentUser().getShortUserName();
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser(userName);
        user = userName;
        groups = Sets.newHashSet(ugi.getGroupNames());
      }

      switch (method.getName()) {
        // SELECT permissions
        case "get_table":
        case "get_partition":
        case "get_partition_with_auth":
        case "get_partition_by_name":
        case "get_partitions":
        case "get_partitions_with_auth":
        case "get_partitions_pspec":
        case "get_partition_names":
        case "get_partitions_ps":
        case "get_partitions_ps_with_auth":
        case "get_partition_names_ps":
        case "get_partitions_by_filter":
        case "get_part_specs_by_filter":
        case "get_partitions_by_names":
        case "get_table_column_statistics":
        case "get_partition_column_statistics":
        case "get_fields_with_environment_context":
        case "get_num_partitions_by_filter":
          this.rangerAccessControlHandler.hasSelectPermission(
                  (String) args[0], (String) args[1], user, groups);
          break;

        // DROP permissions
        case "drop_table":
        case "drop_table_with_environment_context":
          this.rangerAccessControlHandler.hasDropPermission(
                  (String) args[0], (String) args[1], user, groups);
          break;

        // ALTER permissions
        case "alter_table":
        case "alter_table_with_environment_context":
        case "drop_partition":
        case "drop_partition_with_environment_context":
        case "drop_partition_by_name":
        case "drop_partition_by_name_with_environment_context":
        case "alter_partition":
        case "alter_partitions":
        case "alter_partition_with_environment_context":
        case "rename_partition":
        case "delete_partition_column_statistics":
        case "delete_table_column_statistics":
        case "alter_partitions_with_environment_context":
        case "alter_table_with_cascade":
        case "append_partition":
        case "append_partition_with_environment_context":
          this.rangerAccessControlHandler.hasAlterPermission(
                  (String) args[0], (String) args[1], user, groups);
          break;

        // CREATE permissions
        case "create_table":
        case "create_table_with_environment_context":
        case "create_table_with_constraints":
          Table tbl = (Table) args[0];
          this.rangerAccessControlHandler.hasCreatePermission(
                  tbl.getDbName(), tbl.getTableName(), user, groups);
          break;

        // other case
        case "get_partitions_by_expr":
          PartitionsByExprRequest partExprReq = (PartitionsByExprRequest) args[0];
          this.rangerAccessControlHandler.hasSelectPermission(
                  partExprReq.getDbName(), partExprReq.getTblName(), user, groups);
          break;
        case "get_table_statistics_req":
          TableStatsRequest tableStatsRequest = (TableStatsRequest) args[0];
          this.rangerAccessControlHandler.hasSelectPermission(
                  tableStatsRequest.getDbName(), tableStatsRequest.getTblName(), user, groups);
          break;
        case "get_partitions_statistics_req":
        case "get_aggr_stats_for":
          PartitionsStatsRequest partitionsStatsRequest = (PartitionsStatsRequest) args[0];
          this.rangerAccessControlHandler.hasSelectPermission(
                  partitionsStatsRequest.getDbName(), partitionsStatsRequest.getTblName(), user, groups);
          break;
        case "get_table_req":
          GetTableRequest getTableRequest = (GetTableRequest) args[0];
          this.rangerAccessControlHandler.hasSelectPermission(
                  getTableRequest.getDbName(), getTableRequest.getTblName(), user, groups);
          break;
        case "get_partition_values":
          PartitionValuesRequest partitionValuesRequest = (PartitionValuesRequest) args[0];
          this.rangerAccessControlHandler.hasSelectPermission(
                  partitionValuesRequest.getDbName(), partitionValuesRequest.getTblName(), user, groups);
          break;
        case "add_partition":
          Partition partition = (Partition) args[0];
          this.rangerAccessControlHandler.hasAlterPermission(
                  partition.getDbName(), partition.getTableName(), user, groups);
          break;
        case "add_partitions_req":
          AddPartitionsRequest addPartitionsRequest = (AddPartitionsRequest) args[0];
          this.rangerAccessControlHandler.hasAlterPermission(
                  addPartitionsRequest.getDbName(), addPartitionsRequest.getTblName(), user, groups);
          break;
        case "drop_partitions_req":
          DropPartitionsRequest dropPartitionsRequest = (DropPartitionsRequest) args[0];
          this.rangerAccessControlHandler.hasAlterPermission(
                  dropPartitionsRequest.getDbName(), dropPartitionsRequest.getTblName(), user, groups);
          break;
        case "update_table_column_statistics":
        case "update_partition_column_statistics":
          ColumnStatistics columnStatistics = (ColumnStatistics) args[0];
          this.rangerAccessControlHandler.hasAlterPermission(
                  columnStatistics.getStatsDesc().getDbName(),
                  columnStatistics.getStatsDesc().getTableName(), user, groups);
          break;
        // special case, pass it
        case "add_partitions":
        case "add_partitions_pspec":
        case "drop_constraint":
        case "partition_name_has_valid_characters":
        case "set_aggr_stats_for":

        default:
          break;
      }
      return method.invoke(baseHandler, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    } catch (UndeclaredThrowableException e) {
      // Need to unwrap this, so callers get the correct exception thrown by the handler.
      throw e.getCause();
    }
  }

}
