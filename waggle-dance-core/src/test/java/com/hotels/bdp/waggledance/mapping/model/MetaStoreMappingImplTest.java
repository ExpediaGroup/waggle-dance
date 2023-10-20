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
package com.hotels.bdp.waggledance.mapping.model;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.answersWithDelay;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.api.model.ConnectionType.DIRECT;
import static com.hotels.bdp.waggledance.api.model.ConnectionType.TUNNELED;
import static com.hotels.bdp.waggledance.mapping.model.MetaStoreMappingImpl.DEFAULT_AVAILABILITY_TIMEOUT;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.server.security.AccessControlHandler;
import com.hotels.bdp.waggledance.server.security.NotAllowedException;

@RunWith(MockitoJUnitRunner.class)
public class MetaStoreMappingImplTest {

  private static final String DATABASE_PREFIX = "prefix_";
  private static final String NAME = "name";
  private static final long LATENCY = 0;

  private @Mock CloseableThriftHiveMetastoreIface client;
  private @Mock Database database;
  private @Mock AccessControlHandler accessControlHandler;

  private MetaStoreMapping metaStoreMapping;
  private MetaStoreMapping tunneledMetaStoreMapping;

  @Before
  public void init() {
    metaStoreMapping = new MetaStoreMappingImpl(DATABASE_PREFIX, NAME, client, accessControlHandler, DIRECT, LATENCY,
        new DefaultMetaStoreFilterHookImpl(new HiveConf()));
    tunneledMetaStoreMapping = new MetaStoreMappingImpl(DATABASE_PREFIX, NAME, client, accessControlHandler, TUNNELED,
        LATENCY, new DefaultMetaStoreFilterHookImpl(new HiveConf()));
  }

  @Test
  public void transformOutboundDatabaseName() {
    assertThat(metaStoreMapping.transformOutboundDatabaseName("My_Database"), is("my_database"));
  }

  @Test
  public void transformOutboundDatabase() {
    when(database.getName()).thenReturn("My_Database");
    Database outboundDatabase = metaStoreMapping.transformOutboundDatabase(database);
    assertThat(outboundDatabase, is(sameInstance(database)));
    verify(outboundDatabase).setName("my_database");
  }

  @Test
  public void transformInboundDatabaseName() {
    assertThat(metaStoreMapping.transformInboundDatabaseName("My_Database"), is("my_database"));
  }

  @Test
  public void transformInboundDatabaseNameWithoutPrefixReturnsDatabase() {
    assertThat(metaStoreMapping.transformInboundDatabaseName("no_prefix_My_Database"), is("no_prefix_my_database"));
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
  public void isNotAvailableTimeout() throws Exception {
    when(client.isOpen()).then(answersWithDelay(DEFAULT_AVAILABILITY_TIMEOUT+LATENCY+10, a -> true));
    assertThat(metaStoreMapping.isAvailable(), is(false));
  }

  @Test
  public void isAvailableTunnelled() throws Exception {
    when(client.isOpen()).thenReturn(true);
    assertThat(tunneledMetaStoreMapping.isAvailable(), is(true));
    verify(client).getStatus();
  }

  @Test
  public void isNotAvailableTunnelled() throws Exception {
    when(client.isOpen()).thenReturn(false);
    assertThat(tunneledMetaStoreMapping.isAvailable(), is(false));
    verify(client, never()).getStatus();
  }

  @Test
  public void isNotAvailableClientErrorTunnelled() throws Exception {
    when(client.isOpen()).thenReturn(true);
    when(client.getStatus()).thenThrow(new TException("ERROR"));
    assertThat(tunneledMetaStoreMapping.isAvailable(), is(false));
  }

  @Test
  public void checkWritePermissions() {
    String databaseName = "db";
    when(accessControlHandler.hasWritePermission(databaseName)).thenReturn(true);
    assertThat(metaStoreMapping.checkWritePermissions(databaseName), is(metaStoreMapping));
  }

  @Test(expected = NotAllowedException.class)
  public void checkWritePermissionsThrowsException() {
    String databaseName = "db";
    when(accessControlHandler.hasWritePermission(databaseName)).thenReturn(false);
    metaStoreMapping.checkWritePermissions(databaseName);
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

  @Test
  public void getLatency() {
    assertThat(metaStoreMapping.getLatency(), is(LATENCY));
  }
}
