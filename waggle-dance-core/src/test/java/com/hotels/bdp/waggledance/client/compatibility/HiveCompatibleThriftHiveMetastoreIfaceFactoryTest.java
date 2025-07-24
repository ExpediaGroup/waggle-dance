/**
 * Copyright (C) 2016-2025 Expedia, Inc.
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

import static java.util.Collections.emptyList;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.AddCheckConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddDefaultConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddNotNullConstraintRequest;
import org.apache.hadoop.hive.metastore.api.AddUniqueConstraintRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Client;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsResponse;
import org.apache.thrift.TApplicationException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;

@RunWith(MockitoJUnitRunner.class)
public class HiveCompatibleThriftHiveMetastoreIfaceFactoryTest {

  private final HiveCompatibleThriftHiveMetastoreIfaceFactory factory = new HiveCompatibleThriftHiveMetastoreIfaceFactory();

  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "table";
  private @Mock Client delegate;
  private final Table table = new Table(DB_NAME, TABLE_NAME, "", 0, 0, 0, null, null, null, "", "", "");

  @Test
  public void get_table_req() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    GetTableRequest tableRequest = new GetTableRequest(DB_NAME, TABLE_NAME);
    when(delegate.get_table_req(tableRequest)).thenThrow(new TApplicationException("Error"));
    when(delegate.get_table(DB_NAME, TABLE_NAME)).thenReturn(table);
    GetTableResult tableResult = thriftHiveMetastoreIface.get_table_req(tableRequest);
    assertThat(tableResult, is(new GetTableResult(table)));
  }

  @Test
  public void get_table_objects_by_name_req() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    GetTablesRequest tablesRequest = new GetTablesRequest(DB_NAME);
    tablesRequest.addToTblNames(TABLE_NAME);
    when(delegate.get_table_objects_by_name_req(tablesRequest)).thenThrow(new TApplicationException("Error"));
    when(delegate.get_table_objects_by_name(DB_NAME, Lists.newArrayList(TABLE_NAME)))
        .thenReturn(Lists.newArrayList(table));
    GetTablesResult tablesResult = thriftHiveMetastoreIface.get_table_objects_by_name_req(tablesRequest);
    assertThat(tablesResult, is(new GetTablesResult(Lists.newArrayList(table))));
  }

  @Test
  public void normalGetTableCallWorks() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    when(delegate.get_table(DB_NAME, TABLE_NAME)).thenReturn(table);
    Table tableResult = thriftHiveMetastoreIface.get_table(DB_NAME, TABLE_NAME);
    assertThat(tableResult, is(table));
  }

  @Test
  public void underlyingExceptionIsThrownWhenCompatibilityFails() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    TApplicationException cause = new TApplicationException("CAUSE");
    when(delegate.get_all_databases()).thenThrow(cause);
    try {
      thriftHiveMetastoreIface.get_all_databases();
      fail("exception should have been thrown");
    } catch (TApplicationException e) {
      assertThat(e, is(cause));
    }
  }

  @Test
  public void compatibilityExceptionIsThrownWhenCompatibilityFailsOnTException() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    GetTableRequest tableRequest = new GetTableRequest(DB_NAME, TABLE_NAME);
    when(delegate.get_table_req(tableRequest))
        .thenThrow(new TApplicationException("ApplicationException, should not be thrown"));
    NoSuchObjectException cause = new NoSuchObjectException("Should be thrown, this is called from compatiblity layer");
    when(delegate.get_table(DB_NAME, TABLE_NAME)).thenThrow(cause);
    try {
      thriftHiveMetastoreIface.get_table_req(tableRequest);
      fail("exception should have been thrown");
    } catch (NoSuchObjectException e) {
      assertThat(e, is(cause));
    }
  }

  @Test
  public void underlyingyExceptionIsThrownWhenCompatibilityFailsOnTApplication() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    GetTableRequest tableRequest = new GetTableRequest(DB_NAME, TABLE_NAME);
    TApplicationException cause = new TApplicationException("Should be thrown");
    when(delegate.get_table_req(tableRequest)).thenThrow(cause);
    when(delegate.get_table(DB_NAME, TABLE_NAME))
        .thenThrow(new TApplicationException("should not be thrown, this is called from compatiblity layer"));
    try {
      thriftHiveMetastoreIface.get_table_req(tableRequest);
      fail("exception should have been thrown");
    } catch (TApplicationException e) {
      assertThat(e, is(cause));
    }
  }

  @Test
  public void nonTApplicationExceptionsAreThrown() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    GetTableRequest tableRequest = new GetTableRequest(DB_NAME, TABLE_NAME);
    NoSuchObjectException cause = new NoSuchObjectException("Normal Error nothing to do with compatibility");
    when(delegate.get_table_req(tableRequest)).thenThrow(cause);
    try {
      thriftHiveMetastoreIface.get_table_req(tableRequest);
      fail("exception should have been thrown");
    } catch (NoSuchObjectException e) {
      assertThat(e, is(cause));
      verify(delegate, never()).get_table(DB_NAME, TABLE_NAME);
    }
  }

  @Test
  public void get_primary_keys() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    PrimaryKeysRequest primaryKeysRequest = new PrimaryKeysRequest(DB_NAME, TABLE_NAME);
    when(delegate.get_primary_keys(primaryKeysRequest)).thenThrow(new TApplicationException("Error"));
    PrimaryKeysResponse primaryKeysResponse = thriftHiveMetastoreIface.get_primary_keys(primaryKeysRequest);
    assertThat(primaryKeysResponse, is(new PrimaryKeysResponse(emptyList())));
    verify(delegate).get_table(DB_NAME, TABLE_NAME);
  }

  @Test
  public void get_foreign_keys() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    ForeignKeysRequest foreignKeysRequest = new ForeignKeysRequest(null, null, DB_NAME, TABLE_NAME);
    when(delegate.get_foreign_keys(foreignKeysRequest)).thenThrow(new TApplicationException("Error"));
    ForeignKeysResponse foreignKeysResponse = thriftHiveMetastoreIface.get_foreign_keys(foreignKeysRequest);
    assertThat(foreignKeysResponse, is(new ForeignKeysResponse(emptyList())));
    verify(delegate).get_table(DB_NAME, TABLE_NAME);
  }

  @Test
  public void noSuchMethodInCompatibilityLayerHandling() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    when(delegate.get_database(DB_NAME)).thenThrow(new TApplicationException("Error"));
    try {
      // get_database doesn't exist in compatibility layer
      thriftHiveMetastoreIface.get_database(DB_NAME);
      fail("exception should have been thrown");
    } catch (TApplicationException e) {
      //get original TApplicationException back.
      assertThat(e.getMessage(), is("Error"));
    }
  }

  // Hive3
  @Test
  public void create_table_with_constraints() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    List<SQLPrimaryKey> primaryKeys = new ArrayList<>();
    List<SQLForeignKey> foreignKeys = emptyList();
    List<SQLUniqueConstraint> uniqueConstraints = emptyList();
    List<SQLNotNullConstraint> notNullConstraints = emptyList();
    List<SQLDefaultConstraint> defaultConstraints = emptyList();
    List<SQLCheckConstraint> checkConstraints = emptyList();
    doThrow(new TApplicationException("Error"))
        .when(delegate)
        .create_table_with_constraints(table, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints,
            defaultConstraints, checkConstraints);
    thriftHiveMetastoreIface
        .create_table_with_constraints(table, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints,
            defaultConstraints, checkConstraints);
    verify(delegate).create_table(table);
  }

  @Test
  public void add_unique_constraint() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    AddUniqueConstraintRequest req = new AddUniqueConstraintRequest();
    doThrow(new TApplicationException("Error")).when(delegate).add_unique_constraint(req);
    try {
      thriftHiveMetastoreIface.add_unique_constraint(req);
    } catch (Exception e) {
      fail("Should not throw exception");
    }
  }

  @Test
  public void add_not_null_constraint() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    AddNotNullConstraintRequest req = new AddNotNullConstraintRequest();
    doThrow(new TApplicationException("Error")).when(delegate).add_not_null_constraint(req);
    try {
      thriftHiveMetastoreIface.add_not_null_constraint(req);
    } catch (Exception e) {
      fail("Should not throw exception");
    }
  }

  @Test
  public void add_default_constraint() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    AddDefaultConstraintRequest req = new AddDefaultConstraintRequest();
    doThrow(new TApplicationException("Error")).when(delegate).add_default_constraint(req);
    try {
      thriftHiveMetastoreIface.add_default_constraint(req);
    } catch (Exception e) {
      fail("Should not throw exception");
    }
  }

  @Test
  public void add_check_constraint() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    AddCheckConstraintRequest req = new AddCheckConstraintRequest();
    doThrow(new TApplicationException("Error")).when(delegate).add_check_constraint(req);
    try {
      thriftHiveMetastoreIface.add_check_constraint(req);
    } catch (Exception e) {
      fail("Should not throw exception");
    }
  }

  @Test
  public void get_unique_constraints() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    UniqueConstraintsRequest req = new UniqueConstraintsRequest();
    doThrow(new TApplicationException("Error")).when(delegate).get_unique_constraints(req);
    UniqueConstraintsResponse response = thriftHiveMetastoreIface.get_unique_constraints(req);
    UniqueConstraintsResponse emptyResponse = new UniqueConstraintsResponse(emptyList());
    assertThat(response, is(emptyResponse));
  }

  @Test
  public void get_not_null_constraints() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    NotNullConstraintsRequest req = new NotNullConstraintsRequest();
    doThrow(new TApplicationException("Error")).when(delegate).get_not_null_constraints(req);
    NotNullConstraintsResponse response = thriftHiveMetastoreIface.get_not_null_constraints(req);
    NotNullConstraintsResponse emptyResponse = new NotNullConstraintsResponse(emptyList());
    assertThat(response, is(emptyResponse));
  }

  @Test
  public void get_default_constraints() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    DefaultConstraintsRequest req = new DefaultConstraintsRequest();
    doThrow(new TApplicationException("Error")).when(delegate).get_default_constraints(req);
    DefaultConstraintsResponse response = thriftHiveMetastoreIface.get_default_constraints(req);
    DefaultConstraintsResponse emptyResponse = new DefaultConstraintsResponse(emptyList());
    assertThat(response, is(emptyResponse));
  }

  @Test
  public void get_check_constraints() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    CheckConstraintsRequest req = new CheckConstraintsRequest();
    doThrow(new TApplicationException("Error")).when(delegate).get_check_constraints(req);
    CheckConstraintsResponse response = thriftHiveMetastoreIface.get_check_constraints(req);
    CheckConstraintsResponse emptyResponse = new CheckConstraintsResponse(emptyList());
    assertThat(response, is(emptyResponse));
  }

}
