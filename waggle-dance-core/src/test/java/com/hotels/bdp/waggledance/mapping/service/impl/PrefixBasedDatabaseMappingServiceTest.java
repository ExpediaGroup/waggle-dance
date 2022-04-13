/**
 * Copyright (C) 2016-2022 Expedia, Inc.
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;
import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newPrimaryInstance;
import static com.hotels.bdp.waggledance.stubs.HiveStubs.newFunction;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.MappedTables;
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
  private static final long LATENCY = 2000;
  public static final String PRIMARY_DB = "primary_db";
  public static final String FEDERATED_DB = "federated_db";
  private final AbstractMetaStore primaryMetastore = newPrimaryInstance("primary", URI);
  private final FederatedMetaStore federatedMetastore = newFederatedInstance(METASTORE_NAME, URI);
  private final List<String> primaryAndFederatedDbs = Lists.newArrayList(PRIMARY_DB, FEDERATED_DB);
  private @Mock MetaStoreMappingFactory metaStoreMappingFactory;
  private @Mock QueryMapping queryMapping;
  private @Mock Iface primaryDatabaseClient;
  private @Mock Iface federatedDatabaseClient;
  private MetaStoreMapping metaStoreMappingPrimary;
  private MetaStoreMapping metaStoreMappingFederated;
  private PrefixBasedDatabaseMappingService service;

  @Before
  public void init() {
    metaStoreMappingPrimary = mockNewMapping(true, "");
    when(metaStoreMappingPrimary.getClient()).thenReturn(primaryDatabaseClient);
    when(metaStoreMappingPrimary.getLatency()).thenReturn(LATENCY);
    metaStoreMappingFederated = mockNewMapping(true, DB_PREFIX);

    when(metaStoreMappingFactory.newInstance(primaryMetastore)).thenReturn(metaStoreMappingPrimary);
    when(metaStoreMappingFactory.newInstance(federatedMetastore)).thenReturn(metaStoreMappingFederated);
    when(metaStoreMappingFederated.getLatency()).thenReturn(LATENCY);

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
    if (Strings.isNullOrEmpty(prefix)) {
      when(result.transformOutboundDatabaseName(anyString())).then(returnsFirstArg());
      when(result.transformInboundDatabaseName(anyString())).then(returnsFirstArg());
      when(result.transformOutboundDatabaseNameMultiple(anyString()))
          .then((Answer<List<String>>) invocation -> Lists.newArrayList((String) invocation.getArguments()[0]));
    }
    return result;
  }

  @Test
  public void onRegister() {
    AbstractMetaStore newMetastore = newFederatedInstance("newName", "abc");
    MetaStoreMapping newMapping = mockNewMapping(true, "newname_");
    when(metaStoreMappingFactory.newInstance(newMetastore)).thenReturn(newMapping);
    service.onRegister(newMetastore);
    List<DatabaseMapping> databaseMappings = service.getAvailableDatabaseMappings();
    assertThat(databaseMappings.size(), is(3));
    assertThat(ImmutableSet
        .of(databaseMappings.get(0).getDatabasePrefix(), databaseMappings.get(1).getDatabasePrefix(),
            databaseMappings.get(2).getDatabasePrefix()),
        is(ImmutableSet.of("", DB_PREFIX, "newname_")));
  }

  @Test(expected = WaggleDanceException.class)
  public void onRegisterPreviousMappingThrowsException() {
    AbstractMetaStore newMetastore = newFederatedInstance(METASTORE_NAME, "abc");
    service.onRegister(newMetastore);
  }

  @Test
  public void onUpdate() throws NoSuchObjectException {
    AbstractMetaStore newMetastore = newFederatedInstance(METASTORE_NAME, "abc");
    MetaStoreMapping newMapping = mockNewMapping(true, DB_PREFIX);
    Iface newClient = Mockito.mock(Iface.class);
    when(newMapping.getClient()).thenReturn(newClient);
    when(metaStoreMappingFactory.newInstance(newMetastore)).thenReturn(newMapping);
    when(metaStoreMappingFactory.prefixNameFor(federatedMetastore)).thenReturn(DB_PREFIX);

    service.onUpdate(federatedMetastore, newMetastore);

    List<DatabaseMapping> databaseMappings = service.getAvailableDatabaseMappings();
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

    List<DatabaseMapping> databaseMappings = service.getAvailableDatabaseMappings();
    assertThat(databaseMappings.size(), is(2));
    assertThat(
        ImmutableSet.of(databaseMappings.get(0).getDatabasePrefix(), databaseMappings.get(1).getDatabasePrefix()),
        is(ImmutableSet.of("", newPrefix)));
  }

  @Test
  public void onInitOverridesDuplicates() {
    List<AbstractMetaStore> duplicates = Arrays
        .asList(primaryMetastore, federatedMetastore, primaryMetastore, federatedMetastore);
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory, duplicates, queryMapping);
    assertThat(service.getAvailableDatabaseMappings().size(), is(2));
  }

  @Test
  public void onInitEmpty() {
    List<AbstractMetaStore> empty = Collections.emptyList();
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
    List<DatabaseMapping> databaseMappings = service.getAvailableDatabaseMappings();
    assertThat(databaseMappings.size(), is(1));
    assertThat(databaseMappings.get(0).getDatabasePrefix(), is(""));
  }

  @Test
  public void onUnregisterPrimary() {
    when(metaStoreMappingFactory.prefixNameFor(primaryMetastore)).thenReturn("");
    service.onUnregister(primaryMetastore);
    List<DatabaseMapping> databaseMappings = service.getAvailableDatabaseMappings();
    assertThat(databaseMappings.size(), is(1));
    assertThat(databaseMappings.get(0).getDatabasePrefix(), is(DB_PREFIX));
  }

  @Test
  public void primaryDatabaseMapping() {
    DatabaseMapping mapping = service.primaryDatabaseMapping();
    assertThat(mapping.getClient(), is(primaryDatabaseClient));
  }

  @Test
  public void databaseMapping() throws NoSuchObjectException {
    DatabaseMapping databaseMapping = service.databaseMapping(DB_PREFIX + "suffix");
    assertThat(databaseMapping.getDatabasePrefix(), is(DB_PREFIX));
  }

  @Test
  public void databaseMappingMapsToEmptyPrefix() throws NoSuchObjectException {
    DatabaseMapping databaseMapping = service.databaseMapping("some_unknown_prefix_db");
    assertThat(databaseMapping.getDatabasePrefix(), is(""));
  }

  @Test
  public void databaseMappingDefaultsToPrimaryWhenNothingMatches() throws NoSuchObjectException {
    DatabaseMapping databaseMapping = service.databaseMapping("some_unknown_prefix_db");
    assertThat(databaseMapping.getDatabasePrefix(), is(""));
  }

  @Test(expected = NoSuchObjectException.class)
  public void databaseMappingDefaultsToPrimaryEvenWhenNothingMatchesAndUnavailable() throws NoSuchObjectException {
    Mockito.reset(metaStoreMappingPrimary);
    when(metaStoreMappingPrimary.isAvailable()).thenReturn(false);
    DatabaseMapping databaseMapping = service.databaseMapping("some_unknown_prefix_db");

    assertThat(databaseMapping.getDatabasePrefix(), is(""));
  }

  @Test(expected = NoSuchObjectException.class)
  public void databaseMappingDoesNotMatchPrimary() throws NoSuchObjectException {
    AbstractMetaStore noMappedDbsPrimary = primaryMetastore;
    noMappedDbsPrimary.setMappedDatabases(Collections.emptyList());
    service.onUpdate(primaryMetastore, noMappedDbsPrimary);
    service.databaseMapping("some_unknown_db");
  }

  @Test(expected = NoSuchObjectException.class)
  public void databaseMappingDoesNotMatchPrimaryWithOtherMappedDbs() throws NoSuchObjectException {
    AbstractMetaStore noMappedDbsPrimary = primaryMetastore;
    noMappedDbsPrimary.setMappedDatabases(Collections.singletonList(PRIMARY_DB));
    service.onUpdate(primaryMetastore, noMappedDbsPrimary);
    service.databaseMapping("some_unknown_db");
  }

  @Test
  public void availableDatabaseMappings() {
    List<DatabaseMapping> databaseMappings = service.getAvailableDatabaseMappings();
    assertThat(databaseMappings.size(), is(2));
    assertThat(
        ImmutableSet.of(databaseMappings.get(0).getDatabasePrefix(), databaseMappings.get(1).getDatabasePrefix()),
        is(ImmutableSet.of("", DB_PREFIX)));
  }

  @Test
  public void allDatabaseMappings() {
    List<DatabaseMapping> databaseMappings = service.getAllDatabaseMappings();
    assertThat(databaseMappings.size(), is(3));
    assertThat(ImmutableSet
        .of(databaseMappings.get(0).getDatabasePrefix(), databaseMappings.get(1).getDatabasePrefix(),
            databaseMappings.get(2).getDatabasePrefix()),
        is(ImmutableSet.of("", DB_PREFIX, "name2_")));
  }

  @Test
  public void close() throws IOException {
    service.close();
    verify(metaStoreMappingPrimary).close();
    verify(metaStoreMappingFederated).close();
  }

  @Test
  public void checkTableAllowedNoMappedTablesConfig() throws NoSuchObjectException {
    DatabaseMapping mapping = service.databaseMapping(PRIMARY_DB);
    service.checkTableAllowed(PRIMARY_DB, "table", mapping);
  }

  @Test(expected = NoSuchObjectException.class)
  public void checkTableAllowedMappedTablesConfigPresentFederated() throws NoSuchObjectException {
    when(metaStoreMappingFederated.transformInboundDatabaseName(DB_PREFIX + FEDERATED_DB)).thenReturn(FEDERATED_DB);
    federatedMetastore.setMappedDatabases(Collections.singletonList(FEDERATED_DB));
    MappedTables mappedTables = new MappedTables(FEDERATED_DB, Lists.newArrayList("table"));
    federatedMetastore.setMappedTables(Collections.singletonList(mappedTables));
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);
    DatabaseMapping mapping = service.databaseMapping(DB_PREFIX + FEDERATED_DB);
    service.checkTableAllowed(DB_PREFIX + FEDERATED_DB, "table_not_mapped", mapping);
  }

  @Test(expected = NoSuchObjectException.class)
  public void checkTableAllowedMappedTablesConfigPresentPrimary() throws NoSuchObjectException {
    primaryMetastore.setMappedDatabases(Collections.singletonList(PRIMARY_DB));
    MappedTables mappedTables = new MappedTables(PRIMARY_DB, Lists.newArrayList("table"));
    primaryMetastore.setMappedTables(Collections.singletonList(mappedTables));
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);
    DatabaseMapping mapping = service.databaseMapping(PRIMARY_DB);
    service.checkTableAllowed(PRIMARY_DB, "table_not_mapped", mapping);
  }

  @Test
  public void checkTableAllowedMappedTablesAllowed() throws NoSuchObjectException {
    String otherDb = "other_db";
    primaryMetastore.setMappedDatabases(Lists.newArrayList(PRIMARY_DB, otherDb));
    MappedTables mappedTables1 = new MappedTables(PRIMARY_DB, Lists.newArrayList("table"));
    MappedTables mappedTables2 = new MappedTables(otherDb, Lists.newArrayList("table1"));
    primaryMetastore.setMappedTables(Lists.newArrayList(mappedTables1, mappedTables2));
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);
    DatabaseMapping mapping = service.databaseMapping(PRIMARY_DB);
    service.checkTableAllowed(PRIMARY_DB, "table", mapping);
    mapping = service.databaseMapping(otherDb);
    service.checkTableAllowed(otherDb, "table1", mapping);
  }

  @Test
  public void checkTableAllowedMappedTablesEmptyList() throws NoSuchObjectException {
    primaryMetastore.setMappedDatabases(Lists.newArrayList(PRIMARY_DB));
    primaryMetastore.setMappedTables(Collections.emptyList());
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);
    DatabaseMapping mapping = service.databaseMapping(PRIMARY_DB);
    service.checkTableAllowed(PRIMARY_DB, "table", mapping);
  }

  @Test
  public void filterTables() throws NoSuchObjectException {
    List<String> allowedTables = Lists.newArrayList("table", "another_table");
    primaryMetastore.setMappedDatabases(Collections.singletonList(PRIMARY_DB));
    MappedTables mappedTables = new MappedTables(PRIMARY_DB, allowedTables);
    primaryMetastore.setMappedTables(Collections.singletonList(mappedTables));
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);
    DatabaseMapping mapping = service.databaseMapping(PRIMARY_DB);
    List<String> result = service
        .filterTables(PRIMARY_DB, Lists.newArrayList("table", "table_not_mapped", "another_table"), mapping);
    assertThat(result, is(allowedTables));
  }

  @Test
  public void closeOnEmptyInit() throws Exception {
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory, Collections.emptyList(), queryMapping);
    service.close();
    verify(metaStoreMappingPrimary, never()).close();
    verify(metaStoreMappingFederated, never()).close();
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabases() throws Exception {
    when(primaryDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList(PRIMARY_DB));

    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(metaStoreMappingFederated.transformOutboundDatabaseNameMultiple(FEDERATED_DB))
        .thenReturn(Lists.newArrayList(FEDERATED_DB));
    when(federatedDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList(FEDERATED_DB));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    assertThat(handler.getAllDatabases(), is(primaryAndFederatedDbs));
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabasesWithMappedDatabases() throws Exception {
    federatedMetastore.setMappedDatabases(Lists.newArrayList(FEDERATED_DB));
    primaryMetastore.setMappedDatabases(Lists.newArrayList(PRIMARY_DB));
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(metaStoreMappingFederated.transformOutboundDatabaseNameMultiple(FEDERATED_DB))
        .thenReturn(Lists.newArrayList(FEDERATED_DB));
    when(primaryDatabaseClient.get_all_databases())
        .thenReturn(Lists.newArrayList(PRIMARY_DB, "another_db_that_is_not_mapped"));
    when(federatedDatabaseClient.get_all_databases())
        .thenReturn(Lists.newArrayList(FEDERATED_DB, "another_db_that_is_not_mapped"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    assertThat(handler.getAllDatabases(), is(primaryAndFederatedDbs));
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabasesWithEmptyMappedDatabases() throws Exception {
    federatedMetastore.setMappedDatabases(Collections.emptyList());
    primaryMetastore.setMappedDatabases(Collections.emptyList());
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(primaryDatabaseClient.get_all_databases())
        .thenReturn(Lists.newArrayList(PRIMARY_DB, "primary_db_that_is_not_mapped"));
    when(federatedDatabaseClient.get_all_databases())
        .thenReturn(Lists.newArrayList(FEDERATED_DB, "another_db_that_is_not_mapped"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    assertThat(handler.getAllDatabases(), is(Collections.emptyList()));
  }

  @Test
  public void panopticStoreOperationsHandlerGetAllDatabasesByPattern() throws Exception {
    String pattern = "*_db";
    when(primaryDatabaseClient.get_databases(pattern)).thenReturn(Lists.newArrayList(PRIMARY_DB));

    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(metaStoreMappingFederated.transformOutboundDatabaseNameMultiple(FEDERATED_DB))
        .thenReturn(Lists.newArrayList(FEDERATED_DB));
    when(federatedDatabaseClient.get_databases(pattern)).thenReturn(Lists.newArrayList(FEDERATED_DB));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = handler.getAllDatabases(pattern);
    assertThat(allDatabases.size(), is(2));
    assertThat(allDatabases, is(primaryAndFederatedDbs));
  }

  @Test
  public void panopticStoreOperationsHandlerGetAllDatabasesByPatternWithMappedDatabases() throws Exception {
    federatedMetastore.setMappedDatabases(Lists.newArrayList(FEDERATED_DB));
    primaryMetastore.setMappedDatabases(Lists.newArrayList(PRIMARY_DB));
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    String pattern = "*_db";

    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(metaStoreMappingFederated.transformOutboundDatabaseNameMultiple(FEDERATED_DB))
        .thenReturn(Lists.newArrayList(FEDERATED_DB));
    when(primaryDatabaseClient.get_databases(pattern))
        .thenReturn(Lists.newArrayList(PRIMARY_DB, "primary_db_that_is_not_mapped_and_ends_with_db"));
    when(federatedDatabaseClient.get_databases(pattern))
        .thenReturn(Lists.newArrayList(FEDERATED_DB, "another_db_that_is_not_mapped_and_ends_with_db"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = handler.getAllDatabases(pattern);
    assertThat(allDatabases.size(), is(2));
    assertThat(allDatabases, is(primaryAndFederatedDbs));
  }

  @Test
  public void panopticStoreOperationsHandlerGetAllDatabasesByPatternWithEmptyMappedDatabases() throws Exception {
    federatedMetastore.setMappedDatabases(Collections.emptyList());
    primaryMetastore.setMappedDatabases(Collections.emptyList());
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    String pattern = "*_db";

    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(primaryDatabaseClient.get_databases(pattern)).thenReturn(Collections.singletonList(PRIMARY_DB));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = handler.getAllDatabases(pattern);
    assertThat(allDatabases.size(), is(0));
    assertThat(allDatabases, is(Collections.emptyList()));
  }

  @Test
  public void panopticOperationsHandlerGetTableMeta() throws Exception {
    TableMeta federatedTableMeta = new TableMeta(FEDERATED_DB, "tbl", null);
    TableMeta primaryTableMeta = new TableMeta(PRIMARY_DB, "tbl", null);

    when(primaryDatabaseClient.get_table_meta("*_db", "*", null))
        .thenReturn(Collections.singletonList(primaryTableMeta));
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.get_table_meta("*_db", "*", null))
        .thenReturn(Collections.singletonList(federatedTableMeta));
    when(metaStoreMappingFederated.transformOutboundDatabaseName(FEDERATED_DB)).thenReturn("name_federated_db");

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<TableMeta> expected = Arrays.asList(primaryTableMeta, federatedTableMeta);
    List<TableMeta> result = handler.getTableMeta("*_db", "*", null);
    assertThat(result, is(expected));
  }

  @Test
  public void panopticOperationsHandlerGetTableMetaWithMappedTables() throws Exception {
    MappedTables mappedTablesFederated = new MappedTables(FEDERATED_DB, Collections.singletonList("tbl"));
    MappedTables mappedTablesPrimary = new MappedTables(PRIMARY_DB, Collections.singletonList("no_match"));
    federatedMetastore.setMappedTables(Lists.newArrayList(mappedTablesFederated));
    primaryMetastore.setMappedTables(Lists.newArrayList(mappedTablesPrimary));
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    TableMeta federatedTableMeta = new TableMeta(FEDERATED_DB, "tbl", null);
    TableMeta primaryTableMeta = new TableMeta(PRIMARY_DB, "tbl", null);

    when(primaryDatabaseClient.get_table_meta("*_db", "*", null))
        .thenReturn(Collections.singletonList(primaryTableMeta));
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.get_table_meta("*_db", "*", null))
        .thenReturn(Collections.singletonList(federatedTableMeta));
    when(metaStoreMappingFederated.transformOutboundDatabaseName(FEDERATED_DB)).thenReturn("name_federated_db");

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    // table from primary was filtered out
    List<TableMeta> expected = Collections.singletonList(federatedTableMeta);
    List<TableMeta> result = handler.getTableMeta("*_db", "*", null);
    assertThat(result, is(expected));
  }

  @Test
  public void panopticOperationsHandlerSetUgi() throws Exception {
    String user = "user";
    List<String> groups = Lists.newArrayList();
    when(primaryDatabaseClient.set_ugi(user, groups)).thenReturn(Lists.newArrayList("ugi"));

    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.set_ugi(user, groups)).thenReturn(Lists.newArrayList("ugi", "ugi2"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<DatabaseMapping> databaseMappings = service.getAvailableDatabaseMappings();
    List<String> result = handler.setUgi(user, groups, databaseMappings);
    assertThat(result, is(Arrays.asList("ugi", "ugi2")));
  }

  @Test
  public void panopticOperationsHandlerGetAllFunctions() throws Exception {
    GetAllFunctionsResponse responsePrimary = new GetAllFunctionsResponse();
    responsePrimary.addToFunctions(newFunction("db", "fn1"));
    when(primaryDatabaseClient.get_all_functions()).thenReturn(responsePrimary);

    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    GetAllFunctionsResponse responseFederated = new GetAllFunctionsResponse();
    responseFederated.addToFunctions(newFunction("db", "fn2"));
    when(federatedDatabaseClient.get_all_functions()).thenReturn(responseFederated);

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    GetAllFunctionsResponse result = handler.getAllFunctions(service.getAvailableDatabaseMappings());
    assertThat(result.getFunctionsSize(), is(2));
    assertThat(result.getFunctions().get(0).getFunctionName(), is("fn1"));
    assertThat(result.getFunctions().get(1).getFunctionName(), is("fn2"));
  }

  @Test
  public void panopticOperationsHandlerGetAllFunctionsPrimaryMappingHasPrefix() throws Exception {
    when(metaStoreMappingPrimary.getDatabasePrefix()).thenReturn("prefixed_");
    when(metaStoreMappingPrimary.transformOutboundDatabaseName("db")).thenReturn("prefixed_db");
    when(metaStoreMappingPrimary.transformInboundDatabaseName("prefixed_db")).thenReturn("db");
    GetAllFunctionsResponse responsePrimary = new GetAllFunctionsResponse();
    responsePrimary.addToFunctions(newFunction("db", "fn1"));
    when(primaryDatabaseClient.get_all_functions()).thenReturn(responsePrimary);

    when(metaStoreMappingFederated.transformOutboundDatabaseName("db")).thenReturn(DB_PREFIX + "db");
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    GetAllFunctionsResponse responseFederated = new GetAllFunctionsResponse();
    responseFederated.addToFunctions(newFunction("db", "fn2"));
    when(federatedDatabaseClient.get_all_functions()).thenReturn(responseFederated);

    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);
    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    GetAllFunctionsResponse result = handler.getAllFunctions(service.getAvailableDatabaseMappings());
    assertThat(result.getFunctionsSize(), is(3));
    assertThat(result.getFunctions().get(0).getFunctionName(), is("fn1"));
    assertThat(result.getFunctions().get(0).getDbName(), is("prefixed_db"));
    assertThat(result.getFunctions().get(1).getFunctionName(), is("fn1"));
    assertThat(result.getFunctions().get(1).getDbName(), is("db"));
    assertThat(result.getFunctions().get(2).getFunctionName(), is("fn2"));
    assertThat(result.getFunctions().get(2).getDbName(), is(DB_PREFIX + "db"));
  }

  @Test(expected = NoPrimaryMetastoreException.class)
  public void noPrimaryMappingThrowsException() {
    when(metaStoreMappingFactory.newInstance(federatedMetastore)).thenReturn(metaStoreMappingFederated);
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Collections.singletonList(federatedMetastore), queryMapping);
    service.primaryDatabaseMapping();
  }

  @Test(expected = NoPrimaryMetastoreException.class)
  public void noPrimaryThrowsExceptionForUnmappedDatabase() throws NoSuchObjectException {
    when(metaStoreMappingFactory.newInstance(federatedMetastore)).thenReturn(metaStoreMappingFederated);
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Collections.singletonList(federatedMetastore), queryMapping);
    service.databaseMapping("some_unknown_prefix_db");
  }

  @Test
  public void databaseBelongingToFederatedMetastoreMapsToItWithEmptyPrefix() throws NoSuchObjectException {
    String testDatabase = "testDatabase";

    // set metastore whitelist to be nonempty
    federatedMetastore.setMappedDatabases(Collections.singletonList("testName"));
    metaStoreMappingFederated = mockNewMapping(true, DB_PREFIX);
    when(metaStoreMappingFactory.newInstance(federatedMetastore)).thenReturn(metaStoreMappingFederated);
    when(metaStoreMappingFederated.transformInboundDatabaseName(DB_PREFIX + testDatabase)).thenReturn(testDatabase);

    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    DatabaseMapping mapping = service.databaseMapping(DB_PREFIX + testDatabase);
    assertThat(mapping.getDatabasePrefix(), is(""));
  }

  @Test
  public void panopticOperationsHandlerGetTableMetaWithNonWhitelistedDb() throws TException {
    List<String> tblTypes = Lists.newArrayList();
    TableMeta tableMeta = new TableMeta(FEDERATED_DB, "tbl", null);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.get_table_meta("federated_*", "*", tblTypes))
        .thenReturn(Lists.newArrayList(tableMeta));

    // set metastore whitelist to be nonempty
    federatedMetastore.setMappedDatabases(Collections.singletonList("testName"));
    service = new PrefixBasedDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<TableMeta> tableMetas = handler.getTableMeta("name_federated_*", "*", tblTypes);

    assertThat(tableMetas.size(), is(0));
  }
}
