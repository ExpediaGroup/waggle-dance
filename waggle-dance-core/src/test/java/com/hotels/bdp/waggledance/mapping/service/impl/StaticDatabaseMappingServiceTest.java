/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

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
  private final AbstractMetaStore primaryMetastore = newPrimaryInstance(PRIMARY_NAME, URI);
  private final List<String> mappedFederatedDatabases = Lists.newArrayList("federated_DB");
  private @Mock MetaStoreMappingFactory metaStoreMappingFactory;
  private @Mock Iface primaryDatabaseClient;
  private @Mock Iface federatedDatabaseClient;
  private StaticDatabaseMappingService service;
  private FederatedMetaStore federatedMetastore = newFederatedInstance(FEDERATED_NAME, URI);
  private MetaStoreMapping metaStoreMappingPrimary;
  private MetaStoreMapping metaStoreMappingFederated;

  @Before
  public void init() throws Exception {
    federatedMetastore.setMappedDatabases(mappedFederatedDatabases);

    metaStoreMappingPrimary = mockNewMapping(true, primaryMetastore);
    when(metaStoreMappingPrimary.getClient()).thenReturn(primaryDatabaseClient);
    when(metaStoreMappingPrimary.getTimeout()).thenReturn(800L);
    when(primaryDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList("primary_db"));
    metaStoreMappingFederated = mockNewMapping(true, federatedMetastore);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(metaStoreMappingFederated.getTimeout()).thenReturn(800L);
    when(federatedDatabaseClient.get_all_databases()).thenReturn(mappedFederatedDatabases);

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

  private FederatedMetaStore newFederatedInstanceWithClient(
      String name,
      String uri,
      List<String> mappedDatabases,
      boolean availableMapping)
      throws TException {
    FederatedMetaStore newMetastore = newFederatedInstance(name, uri);
    newMetastore.setMappedDatabases(mappedDatabases);
    MetaStoreMapping newMapping = mockNewMapping(availableMapping, newMetastore);
    when(metaStoreMappingFactory.newInstance(newMetastore)).thenReturn(newMapping);
    when(newMapping.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.get_all_databases()).thenReturn(mappedDatabases);
    return newMetastore;
  }

  @Test
  public void databaseMappingPrimary() {
    DatabaseMapping databaseMapping = service.databaseMapping("some_unknown_non_federated_db");
    assertThat(databaseMapping.getMetastoreMappingName(), is(PRIMARY_NAME));
    assertTrue(databaseMapping instanceof IdentityMapping);
  }

  @Test
  public void databaseMappingFederated() {
    DatabaseMapping databaseMapping = service.databaseMapping("federated_DB");
    assertThat(databaseMapping.getMetastoreMappingName(), is(FEDERATED_NAME));
    assertTrue(databaseMapping instanceof IdentityMapping);
  }

  @Test(expected = WaggleDanceException.class)
  public void validateFederatedMetaStoreClashThrowsException() throws TException {
    metaStoreMappingPrimary = mockNewMapping(true, primaryMetastore);
    when(metaStoreMappingPrimary.getClient()).thenReturn(primaryDatabaseClient);
    when(primaryDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList("db"));
    when(metaStoreMappingFactory.newInstance(primaryMetastore)).thenReturn(metaStoreMappingPrimary);

    federatedMetastore = newFederatedInstanceWithClient(FEDERATED_NAME, URI, Lists.newArrayList("db"), true);

    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore));
  }

  @Test(expected = WaggleDanceException.class)
  public void validatePrimaryMetaStoreClashThrowsException() throws TException {
    federatedMetastore = newFederatedInstanceWithClient(FEDERATED_NAME, URI, Lists.newArrayList("db"), true);

    metaStoreMappingPrimary = mockNewMapping(true, primaryMetastore);
    when(metaStoreMappingPrimary.getClient()).thenReturn(primaryDatabaseClient);
    when(primaryDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList("db"));
    when(metaStoreMappingFactory.newInstance(primaryMetastore)).thenReturn(metaStoreMappingPrimary);

    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(federatedMetastore, primaryMetastore));
  }

  @Test(expected = WaggleDanceException.class)
  public void onRegisterPrimaryThrowsExceptionDueToExistingPrimary() {
    PrimaryMetaStore newMetastore = newPrimaryInstance(PRIMARY_NAME, "abc");
    service.onRegister(newMetastore);
  }

  @Test
  public void onRegister() throws TException {
    FederatedMetaStore newMetastore = newFederatedInstanceWithClient("fed1", "abc", Lists.newArrayList("db1"), true);
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

  @Test(expected = WaggleDanceException.class)
  public void onRegisterAnotherPrimaryThrowsException() {
    PrimaryMetaStore newMetastore = newPrimaryInstance("new_name", "new_uri");
    service.onRegister(newMetastore);
  }

  @Test
  public void onUpdate() throws TException {
    FederatedMetaStore newMetastore = newFederatedInstanceWithClient(FEDERATED_NAME, "abc",
        Lists.newArrayList("db1", "federated_DB"), true);
    service.onUpdate(federatedMetastore, newMetastore);

    DatabaseMapping databaseMapping = service.databaseMapping("db1");
    assertThat(databaseMapping.getMetastoreMappingName(), is(FEDERATED_NAME));
    assertTrue(databaseMapping instanceof IdentityMapping);
    databaseMapping = service.databaseMapping("federated_DB");
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
    databaseMapping = service.databaseMapping("federated_db");
    assertThat(databaseMapping.getMetastoreMappingName(), is(FEDERATED_NAME));
  }

  @Test
  public void onUpdateDifferentName() throws TException {
    String newName = "new";
    FederatedMetaStore newMetastore = newFederatedInstanceWithClient(newName, "abc", mappedFederatedDatabases, true);

    service.onUpdate(federatedMetastore, newMetastore);

    DatabaseMapping databaseMapping = service.databaseMapping("federated_DB");
    assertThat(databaseMapping.getMetastoreMappingName(), is(newName));
    assertTrue(databaseMapping instanceof IdentityMapping);
  }

  @Test(expected = WaggleDanceException.class)
  public void onInitDuplicatesThrowsException() {
    List<AbstractMetaStore> duplicates = Arrays
        .asList(primaryMetastore, federatedMetastore, primaryMetastore, federatedMetastore);
    service = new StaticDatabaseMappingService(metaStoreMappingFactory, duplicates);
  }

  @Test
  public void onInitEmpty() {
    List<AbstractMetaStore> empty = Collections.emptyList();
    try {
      service = new StaticDatabaseMappingService(metaStoreMappingFactory, empty);
    } catch (Exception e) {
      fail("It should not throw any exception, an empty list is ok");
    }
  }

  @Test
  public void onUnregister() {
    service.onUnregister(federatedMetastore);
    DatabaseMapping databaseMapping = service.databaseMapping("federated_DB");
    assertThat(databaseMapping.getMetastoreMappingName(), is(PRIMARY_NAME));
  }

  @Test(expected = NoPrimaryMetastoreException.class)
  public void onUnregisterPrimary() {
    service.onUnregister(primaryMetastore);
    service.databaseMapping("a_primary_db");
  }

  @Test
  public void primaryDatabaseMapping() {
    DatabaseMapping mapping = service.primaryDatabaseMapping();
    assertThat(mapping.getClient(), is(primaryDatabaseClient));
  }

  @Test(expected = NoPrimaryMetastoreException.class)
  public void primaryDatabaseMappingNullThrowsException() {
    service.onUnregister(primaryMetastore);
    service.primaryDatabaseMapping();
  }

  @Test
  public void databaseMappingDefaultsToPrimaryEvenWhenNothingMatchesAndUnavailable() {
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
  public void databaseMappingsIgnoreDisconnected() throws TException {
    FederatedMetaStore newMetastore = newFederatedInstanceWithClient("name2", "abc", Lists.newArrayList("db2"), false);
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
    service = new StaticDatabaseMappingService(metaStoreMappingFactory, Collections.<AbstractMetaStore>emptyList());
    service.close();
    verify(metaStoreMappingPrimary, never()).close();
    verify(metaStoreMappingFederated, never()).close();
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabases() {
    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = Lists.newArrayList("primary_db", "federated_db");
    assertThat(handler.getAllDatabases(), is(allDatabases));
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabasesByPattern() throws Exception {
    String pattern = "pattern";
    when(primaryDatabaseClient.get_databases(pattern)).thenReturn(Lists.newArrayList("primary_db"));
    when(federatedDatabaseClient.get_databases(pattern))
        .thenReturn(Lists.newArrayList("federated_db", "another_db_that_is_not_mapped"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = Lists.newArrayList("primary_db", "federated_db");
    assertThat(handler.getAllDatabases(pattern), is(allDatabases));
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabasesByPatternException() throws Exception {
    String pattern = "pattern";
    when(primaryDatabaseClient.get_databases(pattern)).thenThrow(new TException());

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> result = handler.getAllDatabases(pattern);
    assertThat(result.size(), is(0));
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabasesByPatternWithSlowConnection() throws Exception {
    String pattern = "pattern";
    when(primaryDatabaseClient.get_databases(pattern)).thenReturn(Lists.newArrayList("primary_db"));
    mockSlowConnectionFromFederatedMetastore();

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> result = handler.getAllDatabases(pattern);
    assertThat(result.size(), is(1));
    assertThat(result, is(Collections.singletonList("primary_db")));
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabasesByPatternDifferentTimeouts() throws Exception {
    String pattern = "pattern";
    when(primaryDatabaseClient.get_databases(pattern)).thenReturn(Lists.newArrayList("primary_db"));
    when(metaStoreMappingFederated.getTimeout()).thenReturn(0L);

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> result = handler.getAllDatabases(pattern);
    assertThat(result.size(), is(1));
    assertThat(result, is(Collections.singletonList("primary_db")));
  }

  @Test
  public void panopticOperationsHandlerGetTableMeta() throws Exception {
    String pattern = "pattern";
    TableMeta primaryTableMeta = new TableMeta("primary_db", "tbl", null);
    TableMeta federatedTableMeta = new TableMeta("federated_db", "tbl", null);
    TableMeta ignoredTableMeta = new TableMeta("non_mapped_db", "tbl", null);

    when(primaryDatabaseClient.get_table_meta(pattern, pattern, null)).thenReturn(
        Collections.singletonList(primaryTableMeta));
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.get_table_meta(pattern, pattern, null))
        .thenReturn(Arrays.asList(federatedTableMeta, ignoredTableMeta));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<TableMeta> expected = Lists.newArrayList(primaryTableMeta, federatedTableMeta);
    assertThat(handler.getTableMeta(pattern, pattern, null), is(expected));
  }

  @Test
  public void panopticOperationsHandlerGetTableMetaException() throws Exception {
    String pattern = "pattern";
    List<String> tblTypes = Lists.newArrayList();
    when(primaryDatabaseClient.get_table_meta(pattern, pattern, tblTypes)).thenThrow(new TException());

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<TableMeta> tableMeta = handler.getTableMeta(pattern, pattern, tblTypes);
    assertThat(tableMeta.size(), is(0));
  }

  @Test
  public void panopticOperationsHandlerGetTableMetaWithSlowConnection() throws Exception {
    String pattern = "pattern";
    List<String> tblTypes = Lists.newArrayList();
    TableMeta primaryTableMeta = mockTableMeta("primary_db");

    when(primaryDatabaseClient.get_table_meta(pattern, pattern, tblTypes)).thenReturn(
        Lists.newArrayList(primaryTableMeta));
    mockSlowConnectionFromFederatedMetastore();

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<TableMeta> result = handler.getTableMeta(pattern, pattern, tblTypes);
    assertThat(result.size(), is(1));
    assertThat(result, is(Collections.singletonList(primaryTableMeta)));
  }

  @Test
  public void panopticOperationsHandlerGetTableMetaDifferentTimeouts() throws Exception {
    String pattern = "pattern";
    List<String> tblTypes = Lists.newArrayList();
    TableMeta primaryTableMeta = mockTableMeta("primary_db");

    when(primaryDatabaseClient.get_table_meta(pattern, pattern, tblTypes)).thenReturn(
        Lists.newArrayList(primaryTableMeta));
    when(metaStoreMappingFederated.getTimeout()).thenReturn(0L);

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<TableMeta> result = handler.getTableMeta(pattern, pattern, tblTypes);
    assertThat(result.size(), is(1));
    assertThat(result, is(Collections.singletonList(primaryTableMeta)));
  }

  @Test
  public void panopticOperationsHandlerSetUgi() throws Exception {
    String user = "user";
    List<String> groups = Lists.newArrayList();
    when(primaryDatabaseClient.set_ugi(user, groups)).thenReturn(Lists.newArrayList("ugi"));

    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.set_ugi(user, groups)).thenReturn(Lists.newArrayList("ugi", "ugi2"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> result = handler.setUgi(user, groups);
    assertThat(result, is(Arrays.asList("ugi", "ugi2")));
  }

  @Test
  public void panopticOperationsHandlerSetUgiException() throws Exception {
    String user = "user";
    List<String> groups = Lists.newArrayList();
    when(primaryDatabaseClient.set_ugi(user, groups)).thenThrow(new TException());

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> result = handler.setUgi(user, groups);
    assertThat(result.size(), is(0));
  }

  @Test
  public void panopticOperationsHandlerSetUgiWithSlowConnection() throws Exception {
    String user = "user";
    List<String> groups = Lists.newArrayList();
    when(primaryDatabaseClient.set_ugi(user, groups)).thenReturn(Lists.newArrayList("ugi"));
    when(metaStoreMappingFederated.getTimeout()).thenReturn(0L);

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> result = handler.setUgi(user, groups);
    assertThat(result.size(), is(1));
    assertThat(result, is(Collections.singletonList("ugi")));
  }

  @Test
  public void panopticOperationsHandlerSetUgiDifferentTimeouts() throws Exception {
    String user = "user";
    List<String> groups = Lists.newArrayList();
    when(primaryDatabaseClient.set_ugi(user, groups)).thenReturn(Lists.newArrayList("ugi"));
    mockSlowConnectionFromFederatedMetastore();

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> result = handler.setUgi(user, groups);
    assertThat(result.size(), is(1));
    assertThat(result, is(Collections.singletonList("ugi")));
  }

  private TableMeta mockTableMeta(String databaseName) {
    TableMeta tableMeta = mock(TableMeta.class);
    when(tableMeta.getDbName()).thenReturn(databaseName);
    return tableMeta;
  }

  private void mockSlowConnectionFromFederatedMetastore() {
    when(metaStoreMappingFederated.getClient()).thenAnswer((Answer<Iface>) invocation -> {
      try {
        Thread.sleep(5000L);
        fail();
      } catch (InterruptedException ignored) {}
      return federatedDatabaseClient;
    });
  }
}
