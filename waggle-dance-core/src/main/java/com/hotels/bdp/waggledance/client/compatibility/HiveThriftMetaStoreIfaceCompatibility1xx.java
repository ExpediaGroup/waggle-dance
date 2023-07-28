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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class HiveThriftMetaStoreIfaceCompatibility1xx implements HiveThriftMetaStoreIfaceCompatibility {

  private final ThriftHiveMetastore.Client client;

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
}
