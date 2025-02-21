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
package com.hotels.bdp.waggledance.server.security;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;

@RunWith(MockitoJUnitRunner.class)
public class ReadWriteCreateAccessControlHandlerTest {

  private ReadWriteCreateAccessControlHandler handler;
  private @Mock PrimaryMetaStore primaryMetaStore;
  private @Mock FederationService federationService;
  private @Captor ArgumentCaptor<PrimaryMetaStore> captor;
  private String database = "database";
  private String databasePrefix = "primary_";

  @Before
  public void setUp() {
    handler = new ReadWriteCreateAccessControlHandler(primaryMetaStore, federationService);
    when(primaryMetaStore.getDatabasePrefix()).thenReturn(databasePrefix);
  }

  @Test
  public void hasWritePermission() {
    assertTrue(handler.hasWritePermission("db"));
    assertTrue(handler.hasWritePermission(null));
  }

  @Test
  public void hasCreatePermission() {
    assertTrue(handler.hasCreatePermission());
  }

  @Test
  public void databaseCreatedNotificationPrimaryNoMapped() {
    handler.databaseCreatedNotification(database);

    verify(federationService).update(eq(primaryMetaStore), captor.capture());
    PrimaryMetaStore newPrimaryMetaStore = captor.getValue();
    assertThat(newPrimaryMetaStore.getMappedDatabases().size(), is(1));
    assertThat(newPrimaryMetaStore.getMappedDatabases().get(0), is(database));
    assertThat(newPrimaryMetaStore.getDatabasePrefix(), is(databasePrefix));
  }

  @Test
  public void databaseCreatedNotificationPrimaryHasEmptyMapped() {
    when(primaryMetaStore.getMappedDatabases()).thenReturn(Collections.emptyList());
    handler.databaseCreatedNotification(database);

    verify(federationService).update(eq(primaryMetaStore), captor.capture());
    PrimaryMetaStore newPrimaryMetaStore = captor.getValue();
    assertThat(newPrimaryMetaStore.getMappedDatabases().size(), is(1));
    assertThat(newPrimaryMetaStore.getMappedDatabases().get(0), is(database));
    assertThat(newPrimaryMetaStore.getDatabasePrefix(), is(databasePrefix));
  }

  @Test
  public void databaseCreatedNotificationPrimaryHasNonEmptyMapped() {
    List<String> mappedDatabases = Arrays.asList("db1", "db2", "db3");
    when(primaryMetaStore.getMappedDatabases()).thenReturn(mappedDatabases);
    handler.databaseCreatedNotification(database);

    verify(federationService).update(eq(primaryMetaStore), captor.capture());
    PrimaryMetaStore newPrimaryMetaStore = captor.getValue();
    assertThat(newPrimaryMetaStore.getMappedDatabases().size(), is(mappedDatabases.size() + 1));
    assertThat(newPrimaryMetaStore.getMappedDatabases().get(mappedDatabases.size()), is(database));
    assertThat(newPrimaryMetaStore.getDatabasePrefix(), is(databasePrefix));
  }

}
