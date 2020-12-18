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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MetaStoreMappingDecoratorTest {

  private @Mock MetaStoreMapping metaStoreMapping;
  private @Mock Iface client;
  private MetaStoreMappingDecorator decorator;

  @Before
  public void setUp() {
    decorator = new MetaStoreMappingDecorator(metaStoreMapping) {};
  }

  @Test
  public void checkWritePermissions() throws Exception {
    decorator.checkWritePermissions("db");
    verify(metaStoreMapping).checkWritePermissions("db");
  }

  @Test
  public void close() throws Exception {
    decorator.close();
    verify(metaStoreMapping).close();
  }

  @Test
  public void createDatabase() throws Exception {
    Database database = new Database();
    decorator.createDatabase(database);
    verify(metaStoreMapping).createDatabase(database);
  }

  @Test
  public void getClient() throws Exception {
    when(metaStoreMapping.getClient()).thenReturn(client);
    Iface result = decorator.getClient();
    assertThat(result, is(client));
  }

  @Test
  public void getDatabasePrefix() throws Exception {
    when(metaStoreMapping.getDatabasePrefix()).thenReturn("pre");
    String result = decorator.getDatabasePrefix();
    assertThat(result, is("pre"));
  }

  @Test
  public void getLatency() throws Exception {
    when(metaStoreMapping.getLatency()).thenReturn(1L);
    long result = decorator.getLatency();
    assertThat(result, is(1L));
  }

  @Test
  public void getMetastoreMappingName() throws Exception {
    when(metaStoreMapping.getMetastoreMappingName()).thenReturn("Name");
    String result = decorator.getMetastoreMappingName();
    assertThat(result, is("Name"));
  }

  @Test
  public void isAvailable() throws Exception {
    when(metaStoreMapping.isAvailable()).thenReturn(true);
    boolean result = decorator.isAvailable();
    assertThat(result, is(true));
  }

  @Test
  public void transformInboundDatabaseName() throws Exception {
    when(metaStoreMapping.transformInboundDatabaseName("db")).thenReturn("trans_db");
    String result = decorator.transformInboundDatabaseName("db");
    assertThat(result, is("trans_db"));
  }

  @Test
  public void transformOutboundDatabaseName() throws Exception {
    when(metaStoreMapping.transformOutboundDatabaseName("db")).thenReturn("trans_db");
    String result = decorator.transformOutboundDatabaseName("db");
    assertThat(result, is("trans_db"));
  }

  @Test
  public void transformOutboundDatabase() throws Exception {
    Database database = new Database();
    database.setName("a");
    Database expected = new Database();
    expected.setName("b");
    when(metaStoreMapping.transformOutboundDatabase(database)).thenReturn(expected);
    Database result = decorator.transformOutboundDatabase(database);
    assertThat(result, is(expected));
  }

}
