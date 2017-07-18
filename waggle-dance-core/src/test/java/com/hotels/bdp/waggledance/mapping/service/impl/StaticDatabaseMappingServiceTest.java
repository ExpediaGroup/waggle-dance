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
package com.hotels.bdp.waggledance.mapping.service.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;
import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newPrimaryInstance;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import jersey.repackaged.com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.model.IdentityMapping;
import com.hotels.bdp.waggledance.mapping.model.MetaStoreMapping;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.server.NoPrimaryMetastoreException;

@RunWith(MockitoJUnitRunner.class)
public class StaticDatabaseMappingServiceTest {

  private static final String FEDERATED_NAME = "name";
  private static final String PRIMARY_NAME = "primary";
  private static final String URI = "uri";

  private @Mock MetaStoreMappingFactory metaStoreMappingFactory;

  private StaticDatabaseMappingService service;
  private final AbstractMetaStore primaryMetastore = newPrimaryInstance(PRIMARY_NAME, URI);
  private final FederatedMetaStore federatedMetastore = newFederatedInstance(FEDERATED_NAME, URI);
  private @Mock Iface primaryDatabaseClient;
  private MetaStoreMapping metaStoreMappingPrimary;
  private MetaStoreMapping metaStoreMappingFederated;

  @Before
  public void init() throws Exception {
    federatedMetastore.setMappedDatabases(Lists.newArrayList("federatedDB"));
    metaStoreMappingPrimary = mockNewMapping(true, primaryMetastore);
    when(metaStoreMappingPrimary.getClient()).thenReturn(primaryDatabaseClient);
    when(primaryDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList("primary_db"));
    metaStoreMappingFederated = mockNewMapping(true, federatedMetastore);

    when(metaStoreMappingFactory.newInstance(primaryMetastore)).thenReturn(metaStoreMappingPrimary);
    when(metaStoreMappingFactory.newInstance(federatedMetastore)).thenReturn(metaStoreMappingFederated);
    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore));
  }

  private MetaStoreMapping mockNewMapping(boolean isAvailable, AbstractMetaStore metaStore) {
    MetaStoreMapping result = Mockito.mock(MetaStoreMapping.class);
    when(result.isAvailable()).thenReturn(isAvailable);
    when(result.getMetastoreMappingName()).thenReturn(metaStore.getName());
    return result;
  }

  @Test
  public void databaseMappingPrimary() {
    DatabaseMapping databaseMapping = service.databaseMapping("some_unknown_non_federated_db");
    assertThat(databaseMapping.getMetastoreMappingName(), is(PRIMARY_NAME));
    assertTrue(databaseMapping instanceof IdentityMapping);
  }

  @Test
  public void databaseMappingFederated() {
    DatabaseMapping databaseMapping = service.databaseMapping("federatedDB");
    assertThat(databaseMapping.getMetastoreMappingName(), is(FEDERATED_NAME));
    assertTrue(databaseMapping instanceof IdentityMapping);
  }

  @Test(expected = WaggleDanceException.class)
  public void onRegisterPrimaryThrowsExceptionDueToExistingPrimary() {
    PrimaryMetaStore newMetastore = newPrimaryInstance(PRIMARY_NAME, "abc");
    service.onRegister(newMetastore);
  }

  @Test
  public void onRegister() {
    FederatedMetaStore newMetastore = newFederatedInstance("fed1", "abc");
    newMetastore.setMappedDatabases(Lists.newArrayList("db1"));
    MetaStoreMapping newMapping = mockNewMapping(true, newMetastore);
    when(metaStoreMappingFactory.newInstance(newMetastore)).thenReturn(newMapping);
    service.onRegister(newMetastore);
    DatabaseMapping databaseMapping = service.databaseMapping("db1");
    assertThat(databaseMapping.getMetastoreMappingName(), is("fed1"));
    assertTrue(databaseMapping instanceof IdentityMapping);
  }

  @Test(expected = WaggleDanceException.class)
  public void onRegisterPreviousMappingThrowsException() {
    FederatedMetaStore newMetastore = newFederatedInstance(FEDERATED_NAME, "abc");
    service.onRegister(newMetastore);
  }

  @Test
  public void onUpdate() {
    FederatedMetaStore newMetastore = newFederatedInstance(FEDERATED_NAME, "abc");
    newMetastore.setMappedDatabases(Lists.newArrayList("db1", "federatedDB"));
    MetaStoreMapping newMapping = mockNewMapping(true, newMetastore);
    when(metaStoreMappingFactory.newInstance(newMetastore)).thenReturn(newMapping);
    service.onUpdate(federatedMetastore, newMetastore);

    DatabaseMapping databaseMapping = service.databaseMapping("db1");
    assertThat(databaseMapping.getMetastoreMappingName(), is(FEDERATED_NAME));
    assertTrue(databaseMapping instanceof IdentityMapping);
    databaseMapping = service.databaseMapping("federatedDB");
    assertThat(databaseMapping.getMetastoreMappingName(), is(FEDERATED_NAME));
    assertTrue(databaseMapping instanceof IdentityMapping);
  }

  @Test
  public void onUpdatePrimary() throws Exception {
    PrimaryMetaStore newMetastore = newPrimaryInstance("newPrimary", "abc");
    MetaStoreMapping newMapping = mockNewMapping(true, newMetastore);
    Iface newClient = mock(Iface.class);
    when(newClient.get_all_databases()).thenReturn(Lists.newArrayList("primary_db"));
    when(newMapping.getClient()).thenReturn(newClient);
    when(metaStoreMappingFactory.newInstance(newMetastore)).thenReturn(newMapping);

    service.onUpdate(primaryMetastore, newMetastore);

    DatabaseMapping databaseMapping = service.databaseMapping("primary_db");
    assertThat(databaseMapping.getMetastoreMappingName(), is("newPrimary"));
    assertTrue(databaseMapping instanceof IdentityMapping);

    // unchanged
    databaseMapping = service.databaseMapping("federateddb");
    assertThat(databaseMapping.getMetastoreMappingName(), is(FEDERATED_NAME));
  }

  @Test
  public void onUpdateDifferentName() {
    String newName = "new";
    FederatedMetaStore newMetastore = newFederatedInstance(newName, "abc");
    newMetastore.setMappedDatabases(Lists.newArrayList("federatedDB"));
    MetaStoreMapping newMapping = mockNewMapping(true, newMetastore);
    when(metaStoreMappingFactory.newInstance(newMetastore)).thenReturn(newMapping);

    service.onUpdate(federatedMetastore, newMetastore);

    DatabaseMapping databaseMapping = service.databaseMapping("federatedDB");
    assertThat(databaseMapping.getMetastoreMappingName(), is(newName));
    assertTrue(databaseMapping instanceof IdentityMapping);
  }

  @Test(expected = WaggleDanceException.class)
  public void onInitDuplicatesThrowsException() throws Exception {
    List<AbstractMetaStore> duplicates = Arrays.asList(primaryMetastore, federatedMetastore, primaryMetastore,
        federatedMetastore);
    service = new StaticDatabaseMappingService(metaStoreMappingFactory, duplicates);
  }

  @Test
  public void onInitEmpty() throws Exception {
    List<AbstractMetaStore> empty = Collections.<AbstractMetaStore> emptyList();
    try {
      service = new StaticDatabaseMappingService(metaStoreMappingFactory, empty);
    } catch (Exception e) {
      fail("It should not throw any exception, an empty list is ok");
    }
  }

  @Test
  public void onUnregister() {
    service.onUnregister(federatedMetastore);
    DatabaseMapping databaseMapping = service.databaseMapping("federatedDB");
    assertThat(databaseMapping.getMetastoreMappingName(), is(PRIMARY_NAME));
  }

  @Test(expected = NoPrimaryMetastoreException.class)
  public void onUnregisterPrimary() {
    service.onUnregister(primaryMetastore);
    service.databaseMapping("a_primary_db");
  }

  @Test
  public void primaryDatabaseMapping() throws Exception {
    DatabaseMapping mapping = service.primaryDatabaseMapping();
    assertThat(mapping.getClient(), is(primaryDatabaseClient));
  }

  @Test
  public void databaseMappingDefaultsToPrimaryEvenWhenNothingMatchesAndUnavailable() throws Exception {
    AbstractMetaStore newPrimary = newPrimaryInstance("primary", "abc");
    MetaStoreMapping unavailablePrimaryMapping = mockNewMapping(false, newPrimary);
    when(metaStoreMappingFactory.newInstance(newPrimary)).thenReturn(unavailablePrimaryMapping);
    when(unavailablePrimaryMapping.getClient()).thenReturn(primaryDatabaseClient);

    service.onUpdate(primaryMetastore, newPrimary);
    DatabaseMapping databaseMapping = service.databaseMapping("some_unknown_prefix_db");
    assertThat(databaseMapping.getMetastoreMappingName(), is(PRIMARY_NAME));
    assertThat(databaseMapping.isAvailable(), is(false));
  }

  @Test
  public void databaseMappingsIgnoreDisconnected() {
    FederatedMetaStore newMetastore = newFederatedInstance("name2", "abc");
    newMetastore.setMappedDatabases(Lists.newArrayList("db2"));
    MetaStoreMapping newUnavailableMapping = mockNewMapping(false, newMetastore);
    when(metaStoreMappingFactory.newInstance(newMetastore)).thenReturn(newUnavailableMapping);
    service.onRegister(newMetastore);

    DatabaseMapping databaseMapping = service.databaseMapping("db2");
    // Mapping for db2 unavailable so fall back to primary
    assertThat(databaseMapping.getMetastoreMappingName(), is(PRIMARY_NAME));
  }

  @Test
  public void close() throws IOException {
    service.close();
    verify(metaStoreMappingPrimary).close();
    verify(metaStoreMappingFederated).close();
  }

  public void closeOnEmptyInit() throws Exception {
    service = new StaticDatabaseMappingService(metaStoreMappingFactory, Collections.<AbstractMetaStore> emptyList());
    service.close();
    verify(metaStoreMappingPrimary, never()).close();
    verify(metaStoreMappingFederated, never()).close();
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabases() {
    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = Lists.newArrayList("primary_db", "federateddb");
    assertThat(handler.getAllDatabases(), is(allDatabases));
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabasesByPattern() throws Exception {
    String pattern = "pattern";
    when(primaryDatabaseClient.get_databases(pattern)).thenReturn(Lists.newArrayList("primary_db"));

    Iface federatedDatabaseClient = mock(Iface.class);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.get_databases(pattern))
        .thenReturn(Lists.newArrayList("federateddb", "another_db_that_is_not_mapped"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = Lists.newArrayList("primary_db", "federateddb");
    assertThat(handler.getAllDatabases(pattern), is(allDatabases));
  }

  @Test
  public void panopticOperationsHandlerGetTableMeta() throws Exception {
    String pattern = "pattern";
    List<String> tblTypes = Lists.newArrayList();
    TableMeta tableMeta1 = mockTableMeta("primary_db");
    TableMeta tableMeta2 = mockTableMeta("federateddb");
    TableMeta tableMeta3Ignored = mockTableMeta("non_mapped_db");

    when(primaryDatabaseClient.get_table_meta(pattern, pattern, tblTypes)).thenReturn(Lists.newArrayList(tableMeta1));

    Iface federatedDatabaseClient = mock(Iface.class);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.get_table_meta(pattern, pattern, tblTypes))
        .thenReturn(Lists.newArrayList(tableMeta2, tableMeta3Ignored));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<TableMeta> allTableMetas = Lists.newArrayList(tableMeta1, tableMeta2);
    assertThat(handler.getTableMeta(pattern, pattern, tblTypes), is(allTableMetas));
  }

  @Test
  public void panopticOperationsHandlerSetUgi() throws Exception {
    String user = "user";
    List<String> groups = Lists.newArrayList();
    when(primaryDatabaseClient.set_ugi(user, groups)).thenReturn(Lists.newArrayList("ugi"));

    Iface federatedClient = mock(Iface.class);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedClient);
    when(federatedClient.set_ugi(user, groups)).thenReturn(Lists.newArrayList("ugi", "ugi2"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> result = handler.setUgi(user, groups);
    assertThat(result, containsInAnyOrder("ugi", "ugi2"));
  }

  private TableMeta mockTableMeta(String databaseName) {
    TableMeta tableMeta = mock(TableMeta.class);
    when(tableMeta.getDbName()).thenReturn(databaseName);
    return tableMeta;
  }

}
