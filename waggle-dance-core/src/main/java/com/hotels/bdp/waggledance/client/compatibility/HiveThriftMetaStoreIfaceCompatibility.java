/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;

/**
 * This interface contains methods that are missing from Hive 1.x.x but added in later version of Hive
 */
public interface HiveThriftMetaStoreIfaceCompatibility {

  /*
   * https://hive.apache.org/javadocs/r2.3.3/api/org/apache/hadoop/hive/metastore/api/ThriftHiveMetastore.Client.html#
   * get_table_req(org.apache.hadoop.hive.metastore.api.GetTableRequest). Missing in hive 1.x.x
   */
  GetTableResult get_table_req(GetTableRequest req) throws MetaException, NoSuchObjectException, TException;

  /*
   * https://hive.apache.org/javadocs/r2.3.3/api/org/apache/hadoop/hive/metastore/api/ThriftHiveMetastore.Client.html#
   * get_table_objects_by_name_req(org.apache.hadoop.hive.metastore.api.GetTablesRequest). Missing in hive 1.x.x
   */
  GetTablesResult get_table_objects_by_name_req(GetTablesRequest req)
    throws MetaException, InvalidOperationException, UnknownDBException, TException;

  /*
   * https://hive.apache.org/javadocs/r2.3.3/api/org/apache/hadoop/hive/metastore/api/ThriftHiveMetastore.Client.html#
   * get_primary_keys(org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest). Missing in hive 1.x.x
   */
  PrimaryKeysResponse get_primary_keys(PrimaryKeysRequest request)
    throws MetaException, NoSuchObjectException, TException;

  /*
   * https://hive.apache.org/javadocs/r2.3.3/api/org/apache/hadoop/hive/metastore/api/ThriftHiveMetastore.Client.html#
   * get_foreign_keys(org.apache.hadoop.hive.metastore.api.ForeignKeysRequest). Missing in Hive 1.x.x
   */
  ForeignKeysResponse get_foreign_keys(ForeignKeysRequest request)
    throws MetaException, NoSuchObjectException, TException;
}
