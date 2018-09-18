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
package com.hotels.bdp.waggledance.mapping.service.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
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

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.model.MetaStoreMapping;
import com.hotels.bdp.waggledance.mapping.model.QueryMapping;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.server.NoPrimaryMetastoreException;

@RunWith(MockitoJUnitRunner.class)
public class PrefixBasedDatabaseMappingServiceTest {

  private static final String DB_PREFIX = "name_";
  private static final String METASTORE_NAME = "name";
  private static final String URI = "uri";

  private @Mock MetaStoreMappingFactory metaStoreMappingFactory;
  private @Mock QueryMapping queryMapping;

  private PrefixBasedDatabaseMappingService service;
  private final AbstractMetaStore primaryMetastore = newPrimaryInstance("primary", URI);
  private final FederatedMetaStore federatedMetastore = newFederatedInstance(METASTORE_NAME, URI);
  private @Mock Iface primaryDatabaseClient;
  private MetaStoreMapping metaStoreMappingPrimary;
  private MetaStoreMapping metaStoreMappingFederated;

  @Before
  public void init() throws Exception {
    metaStoreMappingPrimary = mockNewMapping(true, "");
    when(metaStoreMappingPrimary.getClient()).thenReturn(primaryDatabaseClient);
    metaStoreMappingFederated = mockNewMapping(true, DB_PREFIX);

    when(metaStoreMappingFactory.newInstance(primaryMetastore)).thenReturn(metaStoreMappingPrimary);
    when(metaStoreMappingFactory.newInstance(federatedMetastore)).thenReturn(metaStoreMappingFederated);

    AbstractMetaStore unavailableMetastore = newFederatedInstance("name2", "thrift:host:port");
    MetaStoreMapping unavailableMapping = mockNewMapping(false, "name2_");
    when(metaStoreMappingFactory.newInstance(unavailableMetastore)).thenReturn(unavailableMapping);

    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore, unavailableMetastore), queryMapping);
  }

  private MetaStoreMapping mockNewMapping(boolean isAvailable, String prefix) {
    MetaStoreMapping result = Mockito.mock(MetaStoreMapping.class);
    when(result.isAvailable()).thenReturn(isAvailable);
    when(result.getDatabasePrefix()).thenReturn(prefix);
    return result;
  }

  @Test
  public void onRegister() {
    AbstractMetaStore newMetastore = newFederatedInstance("newName", "abc");
    MetaStoreMapping newMapping = mockNewMapping(true, "newname_");
    when(metaStoreMappingFactory.newInstance(newMetastore)).thenReturn(newMapping);
    service.onRegister(newMetastore);
    List<DatabaseMapping> databaseMappings = service.databaseMappings();
    assertThat(databaseMappings.size(), is(3));
    assertThat(ImmutableSet.of(databaseMappings.get(0).getDatabasePrefix(), databaseMappings.get(1).getDatabasePrefix(),
        databaseMappings.get(2).getDatabasePrefix()), is(ImmutableSet.of("", DB_PREFIX, "newname_")));
  }

  @Test(expected = WaggleDanceException.class)
  public void onRegisterPreviousMappingThrowsException() {
    AbstractMetaStore newMetastore = newFederatedInstance(METASTORE_NAME, "abc");
    service.onRegister(newMetastore);
  }

  @Test
  public void onUpdate() {
    AbstractMetaStore newMetastore = newFederatedInstance(METASTORE_NAME, "abc");
    MetaStoreMapping newMapping = mockNewMapping(true, DB_PREFIX);
    Iface newClient = Mockito.mock(Iface.class);
    when(newMapping.getClient()).thenReturn(newClient);
    when(metaStoreMappingFactory.newInstance(newMetastore)).thenReturn(newMapping);
    when(metaStoreMappingFactory.prefixNameFor(federatedMetastore)).thenReturn(DB_PREFIX);

    service.onUpdate(federatedMetastore, newMetastore);

    List<DatabaseMapping> databaseMappings = service.databaseMappings();
    assertThat(databaseMappings.size(), is(2));
    assertThat(
        ImmutableSet.of(databaseMappings.get(0).getDatabasePrefix(), databaseMappings.get(1).getDatabasePrefix()),
        is(ImmutableSet.of("", DB_PREFIX)));
    DatabaseMapping databaseMapping = service.databaseMapping(DB_PREFIX);
    assertThat(databaseMapping.getClient(), is(newClient));
  }

  @Test
  public void onUpdateDifferentPrefix() {
    String newPrefix = "newname _";
    AbstractMetaStore newMetastore = newFederatedInstance("newName", "abc");
    MetaStoreMapping newMapping = mockNewMapping(true, newPrefix);
    when(metaStoreMappingFactory.newInstance(newMetastore)).thenReturn(newMapping);
    when(metaStoreMappingFactory.prefixNameFor(federatedMetastore)).thenReturn(DB_PREFIX);

    service.onUpdate(federatedMetastore, newMetastore);

    List<DatabaseMapping> databaseMappings = service.databaseMappings();
    assertThat(databaseMappings.size(), is(2));
    assertThat(
        ImmutableSet.of(databaseMappings.get(0).getDatabasePrefix(), databaseMappings.get(1).getDatabasePrefix()),
        is(ImmutableSet.of("", newPrefix)));
  }

  @Test
  public void onInitOverridesDuplicates() throws Exception {
    List<AbstractMetaStore> duplicates = Arrays.asList(primaryMetastore, federatedMetastore, primaryMetastore,
        federatedMetastore);
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory, duplicates, queryMapping);
    assertThat(service.databaseMappings().size(), is(2));
  }

  @Test
  public void onInitEmpty() throws Exception {
    List<AbstractMetaStore> empty = Collections.<AbstractMetaStore> emptyList();
    try {
      service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory, empty, queryMapping);
    } catch (Exception e) {
      fail("It should not throw any exception, an empty list is ok");
    }
  }

  @Test
  public void onUnregister() {
    when(metaStoreMappingFactory.prefixNameFor(federatedMetastore)).thenReturn(DB_PREFIX);
    service.onUnregister(newFederatedInstance(METASTORE_NAME, URI));
    List<DatabaseMapping> databaseMappings = service.databaseMappings();
    assertThat(databaseMappings.size(), is(1));
    assertThat(databaseMappings.get(0).getDatabasePrefix(), is(""));
  }

  @Test
  public void onUnregisterPrimary() {
    when(metaStoreMappingFactory.prefixNameFor(primaryMetastore)).thenReturn("");
    service.onUnregister(primaryMetastore);
    List<DatabaseMapping> databaseMappings = service.databaseMappings();
    assertThat(databaseMappings.size(), is(1));
    assertThat(databaseMappings.get(0).getDatabasePrefix(), is(DB_PREFIX));
  }

  @Test
  public void primaryDatabaseMapping() throws Exception {
    DatabaseMapping mapping = service.primaryDatabaseMapping();
    assertThat(mapping.getClient(), is(primaryDatabaseClient));
  }

  @Test
  public void databaseMapping() throws Exception {
    DatabaseMapping databaseMapping = service.databaseMapping(DB_PREFIX + "suffix");
    assertThat(databaseMapping.getDatabasePrefix(), is(DB_PREFIX));
  }

  @Test
  public void databaseMappingMapsToEmptyPrefix() throws Exception {
    DatabaseMapping databaseMapping = service.databaseMapping("some_unknown_prefix_db");
    assertThat(databaseMapping.getDatabasePrefix(), is(""));
  }

  @Test
  public void databaseMappingDefaultsToPrimaryWhenNothingMatches() throws Exception {
    DatabaseMapping databaseMapping = service.databaseMapping("some_unknown_prefix_db");

    assertThat(databaseMapping.getDatabasePrefix(), is(""));
  }

  @Test
  public void databaseMappingDefaultsToPrimaryEvenWhenNothingMatchesAndUnavailable() throws Exception {
    Mockito.reset(metaStoreMappingPrimary);
    when(metaStoreMappingPrimary.isAvailable()).thenReturn(false);
    DatabaseMapping databaseMapping = service.databaseMapping("some_unknown_prefix_db");

    assertThat(databaseMapping.getDatabasePrefix(), is(""));
  }

  @Test
  public void databaseMappings() {
    List<DatabaseMapping> databaseMappings = service.databaseMappings();
    assertThat(databaseMappings.size(), is(2));
    assertThat(
        ImmutableSet.of(databaseMappings.get(0).getDatabasePrefix(), databaseMappings.get(1).getDatabasePrefix()),
        is(ImmutableSet.of("", DB_PREFIX)));
  }

  @Test
  public void close() throws IOException {
    service.close();
    verify(metaStoreMappingPrimary).close();
    verify(metaStoreMappingFederated).close();
  }

  public void closeOnEmptyInit() throws Exception {
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Collections.<AbstractMetaStore> emptyList(), queryMapping);
    service.close();
    verify(metaStoreMappingPrimary, never()).close();
    verify(metaStoreMappingFederated, never()).close();
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabases() throws Exception {
    when(primaryDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList("primary_db"));

    Iface federatedClient = mock(Iface.class);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedClient);
    when(metaStoreMappingFederated.transformOutboundDatabaseName("federated_db")).thenReturn("federated_db");
    when(federatedClient.get_all_databases()).thenReturn(Lists.newArrayList("federated_db"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = Lists.newArrayList("primary_db", "federated_db");
    assertThat(handler.getAllDatabases(), is(allDatabases));
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabasesWithMappedDatabases() throws Exception {
    federatedMetastore.setMappedDatabases(Lists.newArrayList("federated_DB"));
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    when(primaryDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList("primary_db"));

    Iface federatedClient = mock(Iface.class);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedClient);
    when(metaStoreMappingFederated.transformOutboundDatabaseName("federated_db")).thenReturn("federated_db");
    when(federatedClient.get_all_databases())
        .thenReturn(Lists.newArrayList("federated_db", "another_db_that_is_not_mapped"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = Lists.newArrayList("primary_db", "federated_db");
    assertThat(handler.getAllDatabases(), is(allDatabases));
  }

  @Test
  public void panopticStoreOperationsHandlerGetAllDatabasesByPattern() throws Exception {
    String pattern = "*_db";
    when(primaryDatabaseClient.get_databases(pattern)).thenReturn(Lists.newArrayList("primary_db"));

    Iface federatedDatabaseClient = mock(Iface.class);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(metaStoreMappingFederated.transformOutboundDatabaseName("federated_db")).thenReturn("federated_db");
    when(federatedDatabaseClient.get_databases(pattern)).thenReturn(Lists.newArrayList("federated_db"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = handler.getAllDatabases(pattern);
    assertThat(allDatabases.size(), is(2));
    assertThat(allDatabases.contains("primary_db"), is(true));
    assertThat(allDatabases.contains("federated_db"), is(true));
  }

  @Test
  public void panopticStoreOperationsHandlerGetAllDatabasesByPatternWithMappedDatabases() throws Exception {
    federatedMetastore.setMappedDatabases(Lists.newArrayList("federated_DB"));
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    String pattern = "*_db";
    when(primaryDatabaseClient.get_databases(pattern)).thenReturn(Lists.newArrayList("primary_db"));

    Iface federatedDatabaseClient = mock(Iface.class);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(metaStoreMappingFederated.transformOutboundDatabaseName("federated_db")).thenReturn("federated_db");
    when(federatedDatabaseClient.get_databases(pattern))
        .thenReturn(Lists.newArrayList("federated_db", "another_db_that_is_not_mapped_and_ends_with_db"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = handler.getAllDatabases(pattern);
    assertThat(allDatabases.size(), is(2));
    assertThat(allDatabases.contains("primary_db"), is(true));
    assertThat(allDatabases.contains("federated_db"), is(true));
  }

  @Test
  public void panopticOperationsHandlerGetTableMeta() throws Exception {
    List<String> tblTypes = Lists.newArrayList();
    TableMeta tableMeta = new TableMeta("federated_db", "tbl", null);

    Iface federatedDatabaseClient = mock(Iface.class);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.get_table_meta("federated_*", "*", tblTypes))
        .thenReturn(Lists.newArrayList(tableMeta));
    when(metaStoreMappingFederated.transformOutboundDatabaseName("federated_db")).thenReturn("name_federated_db");

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<TableMeta> tableMetas = handler.getTableMeta("name_federated_*", "*", tblTypes);
    assertThat(tableMetas.size(), is(1));
    TableMeta tableMetaResult = tableMetas.get(0);
    assertThat(tableMetaResult, is(sameInstance(tableMeta)));
    assertThat(tableMetaResult.getDbName(), is("name_federated_db"));
    assertThat(tableMetaResult.getTableName(), is("tbl"));

    verify(primaryDatabaseClient).get_table_meta("name_federated_*", "*", tblTypes);
    verify(primaryDatabaseClient, never()).get_table_meta("federated_*", "*", tblTypes);
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

  @Test(expected = NoPrimaryMetastoreException.class)
  public void noPrimaryMappingThrowsException() {
    when(metaStoreMappingFactory.newInstance(federatedMetastore)).thenReturn(metaStoreMappingFederated);
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Collections.singletonList((AbstractMetaStore) federatedMetastore), queryMapping);
    service.primaryDatabaseMapping();
  }

  @Test(expected = NoPrimaryMetastoreException.class)
  public void noPrimaryThrowsExceptionForUnmappedDatabase() {
    when(metaStoreMappingFactory.newInstance(federatedMetastore)).thenReturn(metaStoreMappingFederated);
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Collections.singletonList((AbstractMetaStore) federatedMetastore), queryMapping);
    service.databaseMapping("some_unknown_prefix_db");
  }

  @Test
  public void databaseBelongingToFederatedMetastoreMapsToItWithEmptyPrefix() {
    String testDatabase = "testDatabase";

    // set metastore whitelist to be nonempty
    federatedMetastore.setMappedDatabases(Arrays.asList("testName"));
    metaStoreMappingFederated = mockNewMapping(true, DB_PREFIX);
    when(metaStoreMappingFactory.newInstance(federatedMetastore)).thenReturn(metaStoreMappingFederated);
    when(metaStoreMappingFederated.transformInboundDatabaseName(DB_PREFIX + testDatabase)).thenReturn(testDatabase);

    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    DatabaseMapping mapping = service.databaseMapping(DB_PREFIX + testDatabase);
    assertThat(mapping.getDatabasePrefix(), is(""));
  }

  @Test
  public void panopticOperationsHandlerGetTableMetaWithNonWhitelistedDb() throws MetaException, TException {
    List<String> tblTypes = Lists.newArrayList();
    TableMeta tableMeta = new TableMeta("federated_db", "tbl", null);

    Iface federatedDatabaseClient = mock(Iface.class);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.get_table_meta("federated_*", "*", tblTypes))
        .thenReturn(Lists.newArrayList(tableMeta));

    // set metastore whitelist to be nonempty
    federatedMetastore.setMappedDatabases(Arrays.asList("testName"));
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<TableMeta> tableMetas = handler.getTableMeta("name_federated_*", "*", tblTypes);
    assertThat(tableMetas.size(), is(0));
  }

  @Test
  public void panopticOperationsHandlerGetTableMetaLogsException() throws MetaException, TException {
    List<String> tblTypes = Lists.newArrayList();
    TableMeta tableMeta = new TableMeta("federated_db", "tbl", null);

    Iface federatedDatabaseClient = mock(Iface.class);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.get_table_meta("federated_*", "*", tblTypes)).thenThrow(new TException());

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<TableMeta> tableMetas = handler.getTableMeta("name_federated_*", "*", tblTypes);
    assertThat(tableMetas.size(), is(0));
  }

  @Test
  public void panopticStoreOperationsHandlerGetAllDatabasesByPatternLogsException() throws Exception {
    String pattern = "*_db";
    when(primaryDatabaseClient.get_databases(pattern)).thenThrow(new TException());

    Iface federatedDatabaseClient = mock(Iface.class);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.get_databases(pattern)).thenThrow(new TException());

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    assertThat(handler.getAllDatabases(pattern).size(), is(0));
  }

  @Test
  public void panopticStoreOperationsHandlerGetAllDatabasesLogsException() throws Exception {
    when(primaryDatabaseClient.get_all_databases()).thenThrow(new TException());

    Iface federatedClient = mock(Iface.class);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedClient);
    when(federatedClient.get_all_databases()).thenThrow(new TException());

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = Lists.newArrayList("primary_db", "federated_db");
    assertThat(handler.getAllDatabases().size(), is(0));
  }

  @Test
  public void panopticOperationsHandlerSetUgiLogsException() throws Exception {
    String user = "user";
    List<String> groups = Lists.newArrayList();
    when(primaryDatabaseClient.set_ugi(user, groups)).thenThrow(new TException());

    Iface federatedClient = mock(Iface.class);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedClient);
    when(federatedClient.set_ugi(user, groups)).thenThrow(new TException());

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    assertThat(handler.setUgi(user, groups).size(), is(0));
  }
}
