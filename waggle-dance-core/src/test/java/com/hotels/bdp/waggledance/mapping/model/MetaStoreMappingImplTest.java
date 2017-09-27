/**
 * Copyright (C) 2016-2017 Expedia Inc.
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
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.hive.metastore.api.Database;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.server.security.AccessControlHandler;
import com.hotels.bdp.waggledance.server.security.NotAllowedException;

@RunWith(MockitoJUnitRunner.class)
public class MetaStoreMappingImplTest {

  private static final String DATABASE_PREFIX = "prefix_";
  private static final String NAME = "name";

  private @Mock CloseableThriftHiveMetastoreIface client;
  private @Mock Database database;
  private @Mock AccessControlHandler accessControlHandler;

  private MetaStoreMapping metaStoreMapping;

  @Before
  public void init() {
    metaStoreMapping = new MetaStoreMappingImpl(DATABASE_PREFIX, NAME, client, accessControlHandler);
  }

  @Test
  public void transformOutboundDatabaseName() {
    assertThat(metaStoreMapping.transformOutboundDatabaseName("My_Database"), is("prefix_my_database"));
  }

  @Test
  public void transformOutboundDatabase() {
    when(database.getName()).thenReturn("My_Database");
    Database outboundDatabase = metaStoreMapping.transformOutboundDatabase(database);
    assertThat(outboundDatabase, is(sameInstance(database)));
    verify(outboundDatabase).setName("prefix_my_database");
  }

  @Test
  public void transformInboundDatabaseName() {
    assertThat(metaStoreMapping.transformInboundDatabaseName("Prefix_My_Database"), is("my_database"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void transformInboundDatabaseNameFails() {
    metaStoreMapping.transformInboundDatabaseName("waggle_database");
  }

  @Test
  public void getDatabasePrefix() {
    assertThat(metaStoreMapping.getDatabasePrefix(), is(DATABASE_PREFIX));
  }

  @Test
  public void getMetastoreMappingNameSameAsPrefix() {
    assertThat(metaStoreMapping.getMetastoreMappingName(), is(NAME));
  }

  @Test
  public void close() throws IOException {
    metaStoreMapping.close();
    verify(client).close();
  }

  @Test
  public void isAvailable() {
    when(client.isOpen()).thenReturn(true);
    assertThat(metaStoreMapping.isAvailable(), is(true));
  }

  @Test
  public void isNotAvailable() {
    when(client.isOpen()).thenReturn(false);
    assertThat(metaStoreMapping.isAvailable(), is(false));
  }

  @Test
  public void checkWritePermissions() {
    String databaseName = "db";
    when(accessControlHandler.hasWritePermission(databaseName)).thenReturn(true);
    assertThat(metaStoreMapping.checkWritePermissions(DATABASE_PREFIX + databaseName), is(metaStoreMapping));
  }

  @Test(expected = NotAllowedException.class)
  public void checkWritePermissionsThrowsException() {
    String databaseName = "db";
    when(accessControlHandler.hasWritePermission(databaseName)).thenReturn(false);
    metaStoreMapping.checkWritePermissions(DATABASE_PREFIX + databaseName);
  }

  @Test
  public void createDatabase() throws Exception {
    when(database.getName()).thenReturn("db");
    when(accessControlHandler.hasCreatePermission()).thenReturn(true);
    metaStoreMapping.createDatabase(database);
    verify(client).create_database(database);
    verify(accessControlHandler).databaseCreatedNotification("db");
  }

  @Test
  public void createDatabasePermissionDenied() throws Exception {
    when(database.getName()).thenReturn("db");
    when(accessControlHandler.hasCreatePermission()).thenReturn(false);
    try {
      metaStoreMapping.createDatabase(database);
      fail("Should have thrown exception");
    } catch (NotAllowedException e) {
      verify(client, never()).create_database(database);
      verify(accessControlHandler, never()).databaseCreatedNotification("db");
    }
  }
}
