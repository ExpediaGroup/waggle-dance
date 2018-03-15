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
package com.hotels.bdp.waggledance.server.security;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;

@RunWith(MockitoJUnitRunner.class)
public class DatabaseWhitelistAccessControlHandlerTest {

  @Mock
  private PrimaryMetaStore primaryMetaStore;
  @Mock
  private FederationService federationService;
  private DatabaseWhitelistAccessControlHandler handler;
  private final List<String> whitelist = newArrayList("writabledb", "regexdb*");

  @Before
  public void setUp() {
    when(primaryMetaStore.getWritableDatabaseWhiteList()).thenReturn(whitelist);
    handler = new DatabaseWhitelistAccessControlHandler(primaryMetaStore, federationService, true);
  }

  @Test
  public void hasWritePermission() throws Exception {
    assertTrue(handler.hasWritePermission("writableDB"));
    assertTrue(handler.hasWritePermission(null));
    assertFalse(handler.hasWritePermission("nonWritableDB"));
  }

  @Test
  public void hasRegexGrantedWritePermission() throws Exception {
    assertTrue(handler.hasWritePermission("regexDB1"));
    assertTrue(handler.hasWritePermission("regexdb2"));
    assertFalse(handler.hasWritePermission("regex"));
    assertFalse(handler.hasWritePermission("nonregexdb1"));
  }

  @Test
  public void hasRegexGrantedWritePermissionMatchingAllDatabases() throws Exception {
    DatabaseWhitelistAccessControlHandler handlerTwo;
    List<String> whitelistTwo = newArrayList("*");
    PrimaryMetaStore primaryMetaStoreTwo = mock(PrimaryMetaStore.class);
    when(primaryMetaStoreTwo.getWritableDatabaseWhiteList()).thenReturn(whitelistTwo);
    handlerTwo = new DatabaseWhitelistAccessControlHandler(primaryMetaStoreTwo, federationService, true);
    assertTrue(handlerTwo.hasWritePermission("regexDB1"));
    assertTrue(handlerTwo.hasWritePermission("regexdb2"));
    assertTrue(handlerTwo.hasWritePermission("writableDB"));
    assertFalse(handlerTwo.hasWritePermission("nonWritableDB"));
  }

  @Test
  public void hasCreatePermission() throws Exception {
    assertTrue(handler.hasCreatePermission());
    assertTrue(handler.hasWritePermission(null));
    assertTrue(handler.hasCreatePermission());
  }

  @Test
  public void databaseCreatedNotification() throws Exception {
    handler.databaseCreatedNotification("newDB");
    ArgumentCaptor<PrimaryMetaStore> captor = ArgumentCaptor.forClass(PrimaryMetaStore.class);
    verify(federationService).update(eq(primaryMetaStore), captor.capture());
    PrimaryMetaStore updatedMetastore = captor.getValue();
    assertThat(updatedMetastore.getWritableDatabaseWhiteList().size(), is(2));
    assertThat(updatedMetastore.getWritableDatabaseWhiteList(), contains("writabledb", "newdb"));
  }

  @Test
  public void databaseCreatedNotificationNoDuplicates() throws Exception {
    handler.databaseCreatedNotification("writabledb");
    ArgumentCaptor<PrimaryMetaStore> captor = ArgumentCaptor.forClass(PrimaryMetaStore.class);
    verify(federationService).update(eq(primaryMetaStore), captor.capture());
    PrimaryMetaStore updatedMetastore = captor.getValue();
    assertThat(updatedMetastore.getWritableDatabaseWhiteList().size(), is(1));
    assertThat(updatedMetastore.getWritableDatabaseWhiteList(), contains("writabledb"));
  }

}
