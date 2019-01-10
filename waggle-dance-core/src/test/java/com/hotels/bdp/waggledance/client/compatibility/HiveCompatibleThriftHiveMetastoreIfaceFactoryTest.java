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
package com.hotels.bdp.waggledance.client.compatibility;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableResult;
import org.apache.hadoop.hive.metastore.api.GetTablesRequest;
import org.apache.hadoop.hive.metastore.api.GetTablesResult;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Client;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
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
    when(delegate.get_all_databases()).thenThrow(new TApplicationException("CAUSE"));
    try {
      thriftHiveMetastoreIface.get_all_databases();
      fail("exception should have been thrown");
    } catch (TApplicationException e) {
      assertThat(e.getMessage(), is("CAUSE"));
    }
  }

  @Test
  public void compatibilityExceptionIsThrownWhenCompatibilityFailsOnTException() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    GetTableRequest tableRequest = new GetTableRequest(DB_NAME, TABLE_NAME);
    when(delegate.get_table_req(tableRequest))
        .thenThrow(new TApplicationException("ApplicationException, should not be thrown"));
    when(delegate.get_table(DB_NAME, TABLE_NAME))
        .thenThrow(new NoSuchObjectException("Should be thrown, this is called from compatiblity layer"));
    try {
      thriftHiveMetastoreIface.get_table_req(tableRequest);
      fail("exception should have been thrown");
    } catch (NoSuchObjectException e) {
      assertThat(e.getMessage(), is("Should be thrown, this is called from compatiblity layer"));
    }
  }

  @Test
  public void underlyingyExceptionIsThrownWhenCompatibilityFailsOnTApplication() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    GetTableRequest tableRequest = new GetTableRequest(DB_NAME, TABLE_NAME);
    when(delegate.get_table_req(tableRequest)).thenThrow(new TApplicationException("Should be thrown"));
    when(delegate.get_table(DB_NAME, TABLE_NAME))
        .thenThrow(new TApplicationException("should not be thrown, this is called from compatiblity layer"));
    try {
      thriftHiveMetastoreIface.get_table_req(tableRequest);
      fail("exception should have been thrown");
    } catch (TApplicationException e) {
      assertThat(e.getMessage(), is("Should be thrown"));
    }
  }

  @Test
  public void nonTApplicationExceptionsAreThrown() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    GetTableRequest tableRequest = new GetTableRequest(DB_NAME, TABLE_NAME);
    when(delegate.get_table_req(tableRequest))
        .thenThrow(new NoSuchObjectException("Normal Error nothing to do with compatibility"));
    try {
      thriftHiveMetastoreIface.get_table_req(tableRequest);
      fail("exception should have been thrown");
    } catch (TException e) {
      assertThat(e.getMessage(), is("Normal Error nothing to do with compatibility"));
      verify(delegate, never()).get_table(DB_NAME, TABLE_NAME);
    }
  }

  @Test
  public void get_primary_keys() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    PrimaryKeysRequest primaryKeysRequest = new PrimaryKeysRequest(DB_NAME, TABLE_NAME);
    when(delegate.get_primary_keys(primaryKeysRequest)).thenThrow(new TApplicationException("Error"));
    PrimaryKeysResponse primaryKeysResponse = thriftHiveMetastoreIface.get_primary_keys(primaryKeysRequest);
    assertThat(primaryKeysResponse, is(new PrimaryKeysResponse(Collections.emptyList())));
    verify(delegate).get_table(DB_NAME, TABLE_NAME);
  }

  @Test
  public void get_foreign_keys() throws Exception {
    CloseableThriftHiveMetastoreIface thriftHiveMetastoreIface = factory.newInstance(delegate);
    ForeignKeysRequest foreignKeysRequest = new ForeignKeysRequest(null, null, DB_NAME, TABLE_NAME);
    when(delegate.get_foreign_keys(foreignKeysRequest)).thenThrow(new TApplicationException("Error"));
    ForeignKeysResponse foreignKeysResponse = thriftHiveMetastoreIface.get_foreign_keys(foreignKeysRequest);
    assertThat(foreignKeysResponse, is(new ForeignKeysResponse(Collections.emptyList())));
    verify(delegate).get_table(DB_NAME, TABLE_NAME);
  }

}
