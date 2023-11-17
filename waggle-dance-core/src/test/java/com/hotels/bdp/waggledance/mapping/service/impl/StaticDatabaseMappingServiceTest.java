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
package com.hotels.bdp.waggledance.mapping.service.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.AdditionalAnswers.returnsFirstArg;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
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
import java.util.Locale;
import java.util.stream.Collectors;

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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.MappedTables;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMappingImpl;
import com.hotels.bdp.waggledance.mapping.model.MetaStoreMapping;
import com.hotels.bdp.waggledance.mapping.model.QueryMapping;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.server.NoPrimaryMetastoreException;

@RunWith(MockitoJUnitRunner.class)
public class StaticDatabaseMappingServiceTest {

  private static final String FEDERATED_NAME = "name";
  private static final String PRIMARY_NAME = "primary";
  private static final String PRIMARY_DB = "primary_db";
  private static final String FEDERATED_DB = "federated_db";
  private static final String URI = "uri";
  private static final long LATENCY = 2000;
  private final AbstractMetaStore primaryMetastore = newPrimaryInstance(PRIMARY_NAME, URI);
  private final List<String> mappedFederatedDatabases = Lists.newArrayList(FEDERATED_DB);
  private @Mock MetaStoreMappingFactory metaStoreMappingFactory;
  private @Mock Iface primaryDatabaseClient;
  private @Mock Iface federatedDatabaseClient;
  private @Mock QueryMapping queryMapping;
  private StaticDatabaseMappingService service;
  private FederatedMetaStore federatedMetastore = newFederatedInstance(FEDERATED_NAME, URI);
  private MetaStoreMapping metaStoreMappingPrimary;
  private MetaStoreMapping metaStoreMappingFederated;

  @Before
  public void init() throws Exception {
    federatedMetastore.setMappedDatabases(mappedFederatedDatabases);

    metaStoreMappingPrimary = mockNewMapping(true, primaryMetastore);
    when(metaStoreMappingPrimary.getClient()).thenReturn(primaryDatabaseClient);
    when(metaStoreMappingPrimary.getLatency()).thenReturn(LATENCY);
    when(primaryDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList(PRIMARY_DB));
    metaStoreMappingFederated = mockNewMapping(true, federatedMetastore);
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(metaStoreMappingFederated.getLatency()).thenReturn(LATENCY);
    when(federatedDatabaseClient.get_all_databases()).thenReturn(mappedFederatedDatabases);

    when(metaStoreMappingFactory.newInstance(primaryMetastore)).thenReturn(metaStoreMappingPrimary);
    when(metaStoreMappingFactory.newInstance(federatedMetastore)).thenReturn(metaStoreMappingFederated);

    AbstractMetaStore unavailableMetastore = newFederatedInstance("unavailable", "thrift:host:port");
    MetaStoreMapping unavailableMapping = mockNewMapping(false, "unavailable");
    when(metaStoreMappingFactory.newInstance(unavailableMetastore)).thenReturn(unavailableMapping);

    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore, unavailableMetastore), queryMapping);
  }

  private MetaStoreMapping mockNewMapping(boolean isAvailable, String name) {
    MetaStoreMapping result = Mockito.mock(MetaStoreMapping.class);
    when(result.isAvailable()).thenReturn(isAvailable);
    when(result.getMetastoreMappingName()).thenReturn(name);
    return result;
  }

  private MetaStoreMapping mockNewMapping(boolean isAvailable, AbstractMetaStore metaStore) {
    MetaStoreMapping result = Mockito.mock(MetaStoreMapping.class);
    when(result.isAvailable()).thenReturn(isAvailable);
    when(result.getMetastoreMappingName()).thenReturn(metaStore.getName());
    when(result.transformOutboundDatabaseName(anyString())).then(returnsFirstArg());
    when(result.transformOutboundDatabaseNameMultiple(anyString()))
        .then((Answer<List<String>>) invocation -> Lists.newArrayList((String) invocation.getArguments()[0]));
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
    List<String> allLowerCased = mappedDatabases
        .stream()
        .map(s -> s.toLowerCase(Locale.ROOT))
        .collect(Collectors.toList());
    when(federatedDatabaseClient.get_all_databases()).thenReturn(allLowerCased);
    return newMetastore;
  }
  
  @Test
  public void databaseMappingPrimary() throws NoSuchObjectException {
    DatabaseMapping databaseMapping = service.databaseMapping(PRIMARY_DB);
    assertThat(databaseMapping.getMetastoreMappingName(), is(PRIMARY_NAME));
    assertTrue(databaseMapping instanceof DatabaseMappingImpl);
  }

  @Test(expected = NoSuchObjectException.class)
  public void databaseMappingPrimaryNotMatching() throws NoSuchObjectException {
    service.databaseMapping("some_unknown_non_federated_db");
  }

  @Test
  public void databaseMappingFederated() throws NoSuchObjectException {
    service.databaseMapping(FEDERATED_DB);
    DatabaseMapping databaseMapping = service.databaseMapping(FEDERATED_DB);
    assertThat(databaseMapping.getMetastoreMappingName(), is(FEDERATED_NAME));
    assertTrue(databaseMapping instanceof DatabaseMappingImpl);
  }

  @Test(expected = WaggleDanceException.class)
  public void validateFederatedMetaStoreClashThrowsException() throws TException {
    metaStoreMappingPrimary = mockNewMapping(true, primaryMetastore);
    when(metaStoreMappingPrimary.getClient()).thenReturn(primaryDatabaseClient);
    when(primaryDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList("db"));
    when(metaStoreMappingFactory.newInstance(primaryMetastore)).thenReturn(metaStoreMappingPrimary);

    federatedMetastore = newFederatedInstanceWithClient(FEDERATED_NAME, URI, Lists.newArrayList("db"), true);

    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);
  }

  @Test(expected = WaggleDanceException.class)
  public void validateFederatedMetaStoreClashThrowsExceptionFromFederatedClash() throws TException {
    metaStoreMappingPrimary = mockNewMapping(true, primaryMetastore);
    when(metaStoreMappingPrimary.getClient()).thenReturn(primaryDatabaseClient);
    when(primaryDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList("primary_db"));
    when(metaStoreMappingFactory.newInstance(primaryMetastore)).thenReturn(metaStoreMappingPrimary);

    federatedMetastore = newFederatedInstanceWithClient(FEDERATED_NAME, URI, Lists.newArrayList("db"), true);
    AbstractMetaStore secondFederatedMetastore = newFederatedInstanceWithClient("second", URI, Lists.newArrayList("db"),
        true);

    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore, secondFederatedMetastore), queryMapping);
  }

  @Test(expected = WaggleDanceException.class)
  public void validatePrimaryMetaStoreClashThrowsException() throws TException {
    federatedMetastore = newFederatedInstanceWithClient(FEDERATED_NAME, URI, Lists.newArrayList("db"), true);

    metaStoreMappingPrimary = mockNewMapping(true, primaryMetastore);
    when(metaStoreMappingPrimary.getClient()).thenReturn(primaryDatabaseClient);
    when(primaryDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList("db"));
    when(metaStoreMappingFactory.newInstance(primaryMetastore)).thenReturn(metaStoreMappingPrimary);

    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(federatedMetastore, primaryMetastore), queryMapping);
  }
  

  @Test
  public void validateNoPrimaryMetaStoreClashWhenMapped() throws TException {
    federatedMetastore = newFederatedInstanceWithClient(FEDERATED_NAME, URI, Lists.newArrayList("db"), true);

    primaryMetastore.setMappedDatabases(Lists.newArrayList("mapped_db"));
    metaStoreMappingPrimary = mockNewMapping(true, primaryMetastore);
    when(metaStoreMappingPrimary.getClient()).thenReturn(primaryDatabaseClient);
    when(primaryDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList("db", "mapped_db"));
    when(metaStoreMappingFactory.newInstance(primaryMetastore)).thenReturn(metaStoreMappingPrimary);

    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);
  }
  
  @Test
  public void validateNoPrimaryMetaStoreClashWhenMappedPrimarySpecifiedLast() throws TException {
    federatedMetastore = newFederatedInstanceWithClient(FEDERATED_NAME, URI, Lists.newArrayList("db"), true);

    primaryMetastore.setMappedDatabases(Lists.newArrayList("mapped_db"));
    metaStoreMappingPrimary = mockNewMapping(true, primaryMetastore);
    when(metaStoreMappingPrimary.getClient()).thenReturn(primaryDatabaseClient);
    when(primaryDatabaseClient.get_all_databases()).thenReturn(Lists.newArrayList("db", "mapped_db"));
    when(metaStoreMappingFactory.newInstance(primaryMetastore)).thenReturn(metaStoreMappingPrimary);

    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(federatedMetastore, primaryMetastore), queryMapping);
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
    assertTrue(databaseMapping instanceof DatabaseMappingImpl);
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
    assertTrue(databaseMapping instanceof DatabaseMappingImpl);
    databaseMapping = service.databaseMapping(FEDERATED_DB);
    assertThat(databaseMapping.getMetastoreMappingName(), is(FEDERATED_NAME));
    assertTrue(databaseMapping instanceof DatabaseMappingImpl);
  }

  @Test
  public void onUpdatePrimary() throws Exception {
    PrimaryMetaStore newMetastore = newPrimaryInstance("newPrimary", "abc");
    MetaStoreMapping newMapping = mockNewMapping(true, newMetastore);
    Iface newClient = mock(Iface.class);
    when(newClient.get_all_databases()).thenReturn(Lists.newArrayList(PRIMARY_DB));
    when(newMapping.getClient()).thenReturn(newClient);
    when(metaStoreMappingFactory.newInstance(newMetastore)).thenReturn(newMapping);

    service.onUpdate(primaryMetastore, newMetastore);

    DatabaseMapping databaseMapping = service.databaseMapping(PRIMARY_DB);
    assertThat(databaseMapping.getMetastoreMappingName(), is("newPrimary"));
    assertTrue(databaseMapping instanceof DatabaseMappingImpl);

    // unchanged
    databaseMapping = service.databaseMapping(FEDERATED_DB);
    assertThat(databaseMapping.getMetastoreMappingName(), is(FEDERATED_NAME));
  }

  @Test
  public void onUpdateDifferentName() throws TException {
    String newName = "new";
    FederatedMetaStore newMetastore = newFederatedInstanceWithClient(newName, "abc", mappedFederatedDatabases, true);

    service.onUpdate(federatedMetastore, newMetastore);

    DatabaseMapping databaseMapping = service.databaseMapping(FEDERATED_DB);
    assertThat(databaseMapping.getMetastoreMappingName(), is(newName));
    assertTrue(databaseMapping instanceof DatabaseMappingImpl);
  }

  @Test(expected = WaggleDanceException.class)
  public void onInitDuplicatesThrowsException() {
    List<AbstractMetaStore> duplicates = Arrays
        .asList(primaryMetastore, federatedMetastore, primaryMetastore, federatedMetastore);
    service = new StaticDatabaseMappingService(metaStoreMappingFactory, duplicates, queryMapping);
  }

  @Test
  public void onInitEmpty() {
    List<AbstractMetaStore> empty = Collections.emptyList();
    try {
      service = new StaticDatabaseMappingService(metaStoreMappingFactory, empty, queryMapping);
    } catch (Exception e) {
      fail("It should not throw any exception, an empty list is ok");
    }
  }

  @Test(expected = NoSuchObjectException.class)
  public void onUnregister() throws NoSuchObjectException {
    service.onUnregister(federatedMetastore);
    service.databaseMapping(FEDERATED_DB);
  }

  @Test(expected = NoSuchObjectException.class)
  public void onUnregisterPrimary() throws NoSuchObjectException {
    service.onUnregister(primaryMetastore);
    service.databaseMapping(PRIMARY_DB);
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

  @Test(expected = NoSuchObjectException.class)
  public void databaseMappingDoesNotMatchPrimary() throws NoSuchObjectException {
    AbstractMetaStore noMappedDbsPrimary = primaryMetastore;
    noMappedDbsPrimary.setMappedDatabases(Collections.emptyList());
    service.onUpdate(primaryMetastore, noMappedDbsPrimary);
    service.databaseMapping("some_unknown_db");
  }

  @Test(expected = NoSuchObjectException.class)
  public void databaseMappingDoesNotMatchPrimaryWithMappedDbs() throws NoSuchObjectException {
    AbstractMetaStore noMappedDbsPrimary = primaryMetastore;
    noMappedDbsPrimary.setMappedDatabases(Collections.singletonList(PRIMARY_DB));
    service.onUpdate(primaryMetastore, noMappedDbsPrimary);
    service.databaseMapping("some_unknown_db");
  }

  @Test(expected = NoSuchObjectException.class)
  public void databaseMappingDefaultsToPrimaryEvenWhenNothingMatchesAndUnavailable() throws NoSuchObjectException {
    AbstractMetaStore newPrimary = newPrimaryInstance("primary", "abc");
    MetaStoreMapping unavailablePrimaryMapping = mockNewMapping(false, newPrimary);
    when(metaStoreMappingFactory.newInstance(newPrimary)).thenReturn(unavailablePrimaryMapping);

    service.onUpdate(primaryMetastore, newPrimary);
    service.databaseMapping("some_unknown_prefix_db");
  }

  @Test(expected = NoSuchObjectException.class)
  public void databaseMappingsIgnoreDisconnected() throws TException {
    FederatedMetaStore newMetastore = newFederatedInstanceWithClient("name2", "abc", Lists.newArrayList("db2"), false);
    service.onRegister(newMetastore);
    service.databaseMapping("db2");
  }

  @Test
  public void availableDatabaseMappings() {
    List<DatabaseMapping> databaseMappings = service.getAvailableDatabaseMappings();
    assertThat(databaseMappings.size(), is(2));
    assertThat(
        ImmutableSet
            .of(databaseMappings.get(0).getMetastoreMappingName(), databaseMappings.get(1).getMetastoreMappingName()),
        is(ImmutableSet.of(PRIMARY_NAME, FEDERATED_NAME)));
  }

  @Test
  public void allDatabaseMappings() {
    List<DatabaseMapping> databaseMappings = service.getAllDatabaseMappings();
    assertThat(databaseMappings.size(), is(3));
    assertThat(
        ImmutableSet
            .of(databaseMappings.get(0).getMetastoreMappingName(), databaseMappings.get(1).getMetastoreMappingName(),
                databaseMappings.get(2).getMetastoreMappingName()),
        is(ImmutableSet.of(PRIMARY_NAME, FEDERATED_NAME, "unavailable")));
  }

  @Test
  public void checkTableAllowedNoMappedTablesConfig() throws NoSuchObjectException {
    service.checkTableAllowed(PRIMARY_DB, "table", null);
  }

  @Test(expected = NoSuchObjectException.class)
  public void checkTableAllowedMappedTablesConfigPresentFederated() throws NoSuchObjectException {
    federatedMetastore.setMappedDatabases(Collections.singletonList(FEDERATED_DB));
    MappedTables mappedTables = new MappedTables(FEDERATED_DB, Lists.newArrayList("table"));
    federatedMetastore.setMappedTables(Collections.singletonList(mappedTables));
    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);
    service.checkTableAllowed(FEDERATED_DB, "table_not_mapped", null);
  }

  @Test(expected = NoSuchObjectException.class)
  public void checkTableAllowedMappedTablesConfigPresentPrimary() throws NoSuchObjectException {
    primaryMetastore.setMappedDatabases(Collections.singletonList(PRIMARY_DB));
    MappedTables mappedTables = new MappedTables(PRIMARY_DB, Lists.newArrayList("table"));
    primaryMetastore.setMappedTables(Collections.singletonList(mappedTables));
    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);
    service.checkTableAllowed(PRIMARY_DB, "table_not_mapped", null);
  }

  @Test
  public void checkTableAllowedMappedTablesAllowed() throws NoSuchObjectException {
    String otherDb = "other_db";
    primaryMetastore.setMappedDatabases(Lists.newArrayList(PRIMARY_DB, otherDb));
    MappedTables mappedTables1 = new MappedTables(PRIMARY_DB, Lists.newArrayList("table"));
    MappedTables mappedTables2 = new MappedTables(otherDb, Lists.newArrayList("table1"));
    primaryMetastore.setMappedTables(Lists.newArrayList(mappedTables1, mappedTables2));
    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);
    service.checkTableAllowed(PRIMARY_DB, "table", null);
    service.checkTableAllowed(otherDb, "table1", null);
  }

  @Test
  public void checkTableAllowedMappedTablesEmptyList() throws NoSuchObjectException {
    primaryMetastore.setMappedDatabases(Lists.newArrayList(PRIMARY_DB));
    primaryMetastore.setMappedTables(Collections.emptyList());
    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);
    DatabaseMapping mapping = service.databaseMapping(PRIMARY_DB);
    service.checkTableAllowed(PRIMARY_DB, "table", mapping);
  }

  @Test
  public void filterTables() {
    List<String> allowedTables = Lists.newArrayList("table", "another_table");
    primaryMetastore.setMappedDatabases(Collections.singletonList(PRIMARY_DB));
    MappedTables mappedTables = new MappedTables(PRIMARY_DB, allowedTables);
    primaryMetastore.setMappedTables(Collections.singletonList(mappedTables));
    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);
    List<String> result = service
        .filterTables(PRIMARY_DB, Lists.newArrayList("table", "table_not_mapped", "another_table"), null);
    assertThat(result, is(allowedTables));
  }

  @Test
  public void close() throws IOException {
    service.close();
    verify(metaStoreMappingPrimary).close();
    verify(metaStoreMappingFederated).close();
  }

  @Test
  public void closeOnEmptyInit() throws Exception {
    service = new StaticDatabaseMappingService(metaStoreMappingFactory, Collections.emptyList(), queryMapping);
    service.close();
    verify(metaStoreMappingPrimary, never()).close();
    verify(metaStoreMappingFederated, never()).close();
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabases() {
    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = Lists.newArrayList(PRIMARY_DB, FEDERATED_DB);
    assertThat(handler.getAllDatabases(), is(allDatabases));
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabasesWithEmptyMappedDatabases() {
    federatedMetastore.setMappedDatabases(Collections.emptyList());
    primaryMetastore.setMappedDatabases(Collections.emptyList());
    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    assertThat(handler.getAllDatabases(), is(Collections.emptyList()));
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabasesWithMappedDatabases() {
    primaryMetastore.setMappedDatabases(Collections.singletonList(PRIMARY_DB));
    federatedMetastore.setMappedDatabases(Collections.singletonList(FEDERATED_DB));
    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    assertThat(handler.getAllDatabases().size(), is(2));
    assertThat(handler.getAllDatabases(), is(Arrays.asList(PRIMARY_DB, FEDERATED_DB)));
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabasesByPattern() throws Exception {
    String pattern = "pattern";
    when(primaryDatabaseClient.get_databases(pattern)).thenReturn(Lists.newArrayList("primary_db"));
    when(federatedDatabaseClient.get_databases(pattern))
        .thenReturn(Lists.newArrayList(FEDERATED_DB, "another_db_that_is_not_mapped"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabases = Lists.newArrayList(PRIMARY_DB, FEDERATED_DB);
    assertThat(handler.getAllDatabases(pattern), is(allDatabases));
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabasesByPatternWithEmptyMappedDatabases() throws Exception {
    String pattern = "pattern";

    federatedMetastore.setMappedDatabases(Collections.emptyList());
    primaryMetastore.setMappedDatabases(Collections.emptyList());
    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    when(primaryDatabaseClient.get_databases(pattern))
        .thenReturn(Lists.newArrayList(PRIMARY_DB, " primary_db_that_is_not_mapped"));
    when(federatedDatabaseClient.get_databases(pattern))
        .thenReturn(Lists.newArrayList(FEDERATED_DB, "another_db_that_is_not_mapped"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    assertThat(handler.getAllDatabases(pattern), is(Collections.emptyList()));
  }

  @Test
  public void panopticOperationsHandlerGetAllDatabasesByPatternWithMappedDatabases() throws Exception {
    String pattern = "pattern";

    primaryMetastore.setMappedDatabases(Collections.singletonList(PRIMARY_DB));
    federatedMetastore.setMappedDatabases(Collections.singletonList(FEDERATED_DB));
    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    when(primaryDatabaseClient.get_databases(pattern))
        .thenReturn(Lists.newArrayList(PRIMARY_DB, "primary_db_that_is_not_mapped"));
    when(federatedDatabaseClient.get_databases(pattern))
        .thenReturn(Lists.newArrayList(FEDERATED_DB, "another_db_that_is_not_mapped"));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<String> allDatabasesByPattern = handler.getAllDatabases(pattern);
    assertThat(allDatabasesByPattern.size(), is(2));
    assertThat(allDatabasesByPattern, is(Arrays.asList(PRIMARY_DB, FEDERATED_DB)));
  }

  @Test
  public void panopticOperationsHandlerGetTableMeta() throws Exception {
    String pattern = "pattern";
    TableMeta primaryTableMeta = new TableMeta(PRIMARY_DB, "tbl", null);
    TableMeta federatedTableMeta = new TableMeta(FEDERATED_DB, "tbl", null);
    TableMeta ignoredTableMeta = new TableMeta("non_mapped_db", "tbl", null);

    when(primaryDatabaseClient.get_table_meta(pattern, pattern, null))
        .thenReturn(Collections.singletonList(primaryTableMeta));
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.get_table_meta(pattern, pattern, null))
        .thenReturn(Arrays.asList(federatedTableMeta, ignoredTableMeta));

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    List<TableMeta> expected = Lists.newArrayList(primaryTableMeta, federatedTableMeta);
    List<TableMeta> result = handler.getTableMeta(pattern, pattern, null);
    assertThat(result, is(expected));
  }

  @Test
  public void panopticOperationsHandlerGetTableMetaWithMappedTables() throws Exception {
    MappedTables mappedTablesFederated = new MappedTables(FEDERATED_DB, Collections.singletonList("tbl"));
    MappedTables mappedTablesPrimary = new MappedTables(PRIMARY_DB, Collections.singletonList("no_match"));
    federatedMetastore.setMappedTables(Lists.newArrayList(mappedTablesFederated));
    primaryMetastore.setMappedTables(Lists.newArrayList(mappedTablesPrimary));
    service = new StaticDatabaseMappingService(metaStoreMappingFactory,
        Arrays.asList(primaryMetastore, federatedMetastore), queryMapping);

    TableMeta federatedTableMeta = new TableMeta(FEDERATED_DB, "tbl", null);
    TableMeta primaryTableMeta = new TableMeta(PRIMARY_DB, "tbl", null);
    TableMeta ignoredTableMeta = new TableMeta("non_mapped_db", "tbl", null);

    when(primaryDatabaseClient.get_table_meta("*_db", "*", null))
        .thenReturn(Collections.singletonList(primaryTableMeta));
    when(metaStoreMappingFederated.getClient()).thenReturn(federatedDatabaseClient);
    when(federatedDatabaseClient.get_table_meta("*_db", "*", null))
        .thenReturn(Arrays.asList(federatedTableMeta, ignoredTableMeta));
    when(metaStoreMappingFederated.transformOutboundDatabaseName(FEDERATED_DB)).thenReturn("name_federated_db");

    PanopticOperationHandler handler = service.getPanopticOperationHandler();
    // table from primary was filtered out
    List<TableMeta> expected = Arrays.asList(federatedTableMeta);
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
}
