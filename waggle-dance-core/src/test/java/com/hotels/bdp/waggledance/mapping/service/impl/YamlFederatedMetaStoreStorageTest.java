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
package com.hotels.bdp.waggledance.mapping.service.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import javax.validation.ConstraintViolationException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import fm.last.commons.test.file.ClassDataFolder;
import fm.last.commons.test.file.DataFolder;

import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.AccessControlType;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.MappedTables;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;
import com.hotels.bdp.waggledance.conf.YamlStorageConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class YamlFederatedMetaStoreStorageTest {

  public final @Rule TemporaryFolder tmp = new TemporaryFolder();
  public final @Rule DataFolder dataFolder = new ClassDataFolder();

  @Mock
  private YamlStorageConfiguration configuration;

  @Before
  public void init() {
    when(configuration.isOverwriteConfigOnShutdown()).thenReturn(true);
  }

  @Test
  public void loadFederation_empty() throws Exception {
    File f = dataFolder.getFile("empty-file.yml");
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage(f.toURI().toString(), configuration);
    storage.loadFederation();
    assertThat(storage.getAll().size(), is(0));
  }

  @Test
  public void loadFederationNoFederations() throws Exception {
    File f = dataFolder.getFile("no-federations.yml");
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage(f.toURI().toString(), configuration);
    storage.loadFederation();
    assertThat(storage.getAll().size(), is(0));
  }

  @Test
  public void loadFederation_singleFederation() throws Exception {
    File f = dataFolder.getFile("single-federation.yml");
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage(f.toURI().toString(), configuration);
    storage.loadFederation();
    assertThat(storage.getAll(), is(notNullValue()));
    assertThat(storage.getAll().size(), is(1));
    PrimaryMetaStore expected = newPrimaryInstance("name", "thrift://localhost:9083");
    expected.setAccessControlType(AccessControlType.READ_AND_WRITE_ON_DATABASE_WHITELIST);
    expected.setWritableDatabaseWhiteList(Lists.newArrayList("db1", "db2"));
    assertThat(storage.getAll().get(0), is(expected));
    assertThat(storage.get("name"), is(expected));
  }

  @Test(expected = IllegalArgumentException.class)
  public void loadFederationSamePrefix() throws Exception {
    File f = dataFolder.getFile("same-prefix.yml");
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage(f.toURI().toString(), configuration);
    storage.loadFederation();
  }

  @Test(expected = IllegalArgumentException.class)
  public void loadTwoEmptyPrefixes() throws Exception {
    File f = dataFolder.getFile("two-empty-prefixes.yml");
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage(f.toURI().toString(), configuration);
    storage.loadFederation();
  }

  @Test
  public void loadEmptyPrefixFederated() throws Exception {
    File f = dataFolder.getFile("empty-prefix-federated.yml");
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage(f.toURI().toString(), configuration);
    storage.loadFederation();
    assertThat(storage.getAll().size(), is(3));
    assertThat(storage.getAll().get(0).getDatabasePrefix(), is("primary_"));
    assertThat(storage.getAll().get(1).getDatabasePrefix(), is(""));
    assertThat(storage.getAll().get(2).getDatabasePrefix(), is("hcom_2_prefix_"));
  }

  @Test
  public void update() throws Exception {
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage("", configuration);
    PrimaryMetaStore primary = newPrimaryInstance("prefix1", "metastoreUri");
    storage.insert(primary);
    assertThat(storage.getAll().size(), is(1));
    storage.update(primary, newPrimaryInstance("prefix1", "metastoreUriUPDATED"));
    assertThat(storage.getAll().size(), is(1));
    assertThat(storage.getAll().get(0).getRemoteMetaStoreUris(), is("metastoreUriUPDATED"));
  }

  @Test
  public void updateDifferentPrefix() throws Exception {
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage("", configuration);
    PrimaryMetaStore primary = newPrimaryInstance("prefix1", "metastoreUri");
    storage.insert(primary);
    assertThat(storage.getAll().size(), is(1));
    storage.update(primary, newPrimaryInstance("prefix2", "metastoreUriUPDATED"));
    assertThat(storage.getAll().size(), is(1));
    assertThat(storage.getAll().get(0).getRemoteMetaStoreUris(), is("metastoreUriUPDATED"));
  }

  @Test
  public void delete() throws Exception {
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage("", configuration);
    PrimaryMetaStore metaStore = newPrimaryInstance("name1", "metastoreUri");
    storage.insert(metaStore);
    assertThat(storage.getAll().size(), is(1));
    storage.delete(metaStore.getName());
    assertThat(storage.getAll().size(), is(0));
  }

  @Test
  public void loadFederationMultipleFederations() throws Exception {
    File f = dataFolder.getFile("multi-federation.yml");
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage(f.toURI().toString(), configuration);
    storage.loadFederation();
    assertThat(storage.getAll(), is(notNullValue()));
    assertThat(storage.getAll().size(), is(3));
    assertThat(storage.getAll().get(0), is(newPrimaryInstance("hcom_3", "thrift://localhost:39083")));
    assertThat(storage.getAll().get(1), is(newFederatedInstance("hcom_1", "thrift://localhost:19083")));
    FederatedMetaStore metaStore = newFederatedInstance("hcom_2", "thrift://localhost:29083");
    metaStore.setDatabasePrefix("hcom_2_prefix_");
    assertThat(storage.getAll().get(2), is(metaStore));
  }

  @Test
  public void loadFederation_mappedTables() throws Exception {
    File f = dataFolder.getFile("mapped-tables.yml");
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage(f.toURI().toString(), configuration);
    storage.loadFederation();
    assertThat(storage.getAll(), is(notNullValue()));
    assertThat(storage.getAll().size(), is(2));
    assertThat(storage.getAll().get(0), is(newPrimaryInstance("hcom_2", "thrift://localhost:39083")));
    FederatedMetaStore metaStore = newFederatedInstance("hcom_1", "thrift://localhost:19083");
    metaStore.setDatabasePrefix("hcom_1_prefix_");
    metaStore.setMappedDatabases(Lists.newArrayList("db1", "db2"));
    MappedTables mappedTables1 = new MappedTables("db1", Lists.newArrayList("tbl1"));
    MappedTables mappedTables2 = new MappedTables("db2", Lists.newArrayList("tbl2"));
    metaStore.setMappedTables(Lists.newArrayList(mappedTables1, mappedTables2));
    assertThat(storage.getAll().get(1), is(metaStore));
  }

  @Test(expected = ConstraintViolationException.class)
  public void loadFederationInvalidFederation() throws Exception {
    File f = dataFolder.getFile("invalid-federation.yml");
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage(f.toURI().toString(), configuration);
    storage.loadFederation();
  }

  @Test
  public void saveFederationWriteFederations() throws Exception {
    File f = tmp.newFile("federations.yml");
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage(f.toURI().toString(), configuration);
    storage.insert(newPrimaryInstance("hcom_1", "thrift://localhost:19083"));
    FederatedMetaStore newFederatedInstance = newFederatedInstance("hcom_2", "thrift://localhost:29083");
    newFederatedInstance.setMappedDatabases(Lists.newArrayList("db1", "db2"));
    MappedTables mappedTables1 = new MappedTables("db1", Lists.newArrayList("tbl1"));
    MappedTables mappedTables2 = new MappedTables("db2", Lists.newArrayList("tbl2"));
    newFederatedInstance.setMappedTables(Lists.newArrayList(mappedTables1, mappedTables2));
    storage.insert(newFederatedInstance);
    storage.saveFederation();
    List<String> lines = Files.readAllLines(f.toPath(), StandardCharsets.UTF_8);
    assertThat(lines.size(), is(23));
    assertThat(lines.get(0), is("primary-meta-store:"));
    assertThat(lines.get(1), is("  access-control-type: READ_ONLY"));
    assertThat(lines.get(2), is("  database-prefix: ''"));
    assertThat(lines.get(3), is("  latency: 0"));
    assertThat(lines.get(4), is("  name: hcom_1"));
    assertThat(lines.get(5), is("  remote-meta-store-uris: thrift://localhost:19083"));
    assertThat(lines.get(6), is("federated-meta-stores:"));
    assertThat(lines.get(7), is("- access-control-type: READ_ONLY"));
    assertThat(lines.get(8), is("  database-name-mapping: {}"));
    assertThat(lines.get(9), is("  database-prefix: hcom_2_"));
    assertThat(lines.get(10), is("  latency: 0"));
    assertThat(lines.get(11), is("  mapped-databases:"));
    assertThat(lines.get(12), is("  - db1"));
    assertThat(lines.get(13), is("  - db2"));
    assertThat(lines.get(14), is("  mapped-tables:"));
    assertThat(lines.get(15), is("  - database: db1"));
    assertThat(lines.get(16), is("    mapped-tables:"));
    assertThat(lines.get(17), is("    - tbl1"));
    assertThat(lines.get(18), is("  - database: db2"));
    assertThat(lines.get(19), is("    mapped-tables:"));
    assertThat(lines.get(20), is("    - tbl2"));
    assertThat(lines.get(21), is("  name: hcom_2"));
    assertThat(lines.get(22), is("  remote-meta-store-uris: thrift://localhost:29083"));
  }

  @Test
  public void doNotSaveFederationWriteFederations() throws Exception {
    when(configuration.isOverwriteConfigOnShutdown()).thenReturn(false);
    File f = tmp.newFile("federations.yml");
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage(f.toURI().toString(), configuration);
    storage.insert(newPrimaryInstance("hcom_1", "thrift://localhost:19083"));
    FederatedMetaStore newFederatedInstance = newFederatedInstance("hcom_2", "thrift://localhost:29083");
    newFederatedInstance.setMappedDatabases(Lists.newArrayList("db1", "db2"));
    storage.insert(newFederatedInstance);
    storage.saveFederation();
    List<String> lines = Files.readAllLines(f.toPath(), StandardCharsets.UTF_8);
    assertThat(lines.size(), is(0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void insertWithSameNameFails() throws Exception {
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage("", configuration);
    PrimaryMetaStore primary = newPrimaryInstance("primary", "metastoreUri");
    storage.insert(primary);
    FederatedMetaStore federatedWithSameName = AbstractMetaStore.newFederatedInstance(primary.getName(), "uris");
    storage.insert(federatedWithSameName);
  }

  @Test(expected = IllegalArgumentException.class)
  public void insertPrimaryWithSamePrefixFails() throws Exception {
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage("", configuration);
    PrimaryMetaStore primary = newPrimaryInstance("primary", "metastoreUri");
    storage.insert(primary);
    PrimaryMetaStore primaryWithSamePrefix = newPrimaryInstance("newPrimary", "uris");
    storage.insert(primaryWithSamePrefix);
  }

  @Test(expected = IllegalArgumentException.class)
  public void insertFederatedWithSamePrefixFails() throws Exception {
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage("", configuration);
    FederatedMetaStore metaStore = newFederatedInstance("name", "metastoreUri");
    storage.insert(metaStore);
    FederatedMetaStore metaStoreWithSamePrefix = newFederatedInstance("newName", "uris");
    metaStoreWithSamePrefix.setDatabasePrefix(metaStore.getDatabasePrefix());
    storage.insert(metaStoreWithSamePrefix);
  }

  @Test
  public void savePrimaryWriteFederations() throws Exception {
    File f = tmp.newFile("federations.yml");
    YamlFederatedMetaStoreStorage storage = new YamlFederatedMetaStoreStorage(f.toURI().toString(), configuration);
    PrimaryMetaStore primaryMetaStore = newPrimaryInstance("hcom_1", "thrift://localhost:19083");
    primaryMetaStore.setMappedDatabases(Lists.newArrayList("db1", "db2"));
    MappedTables mappedTables1 = new MappedTables("db1", Lists.newArrayList("tbl1"));
    MappedTables mappedTables2 = new MappedTables("db2", Lists.newArrayList("tbl2"));
    primaryMetaStore.setMappedTables(Lists.newArrayList(mappedTables1, mappedTables2));
    storage.insert(primaryMetaStore);
    storage.insert(newFederatedInstance("hcom_2", "thrift://localhost:29083"));
    storage.saveFederation();
    List<String> lines = Files.readAllLines(f.toPath(), StandardCharsets.UTF_8);
    assertThat(lines.size(), is(23));
    assertThat(lines.get(0), is("primary-meta-store:"));
    assertThat(lines.get(1), is("  access-control-type: READ_ONLY"));
    assertThat(lines.get(2), is("  database-prefix: ''"));
    assertThat(lines.get(3), is("  latency: 0"));
    assertThat(lines.get(4), is("  mapped-databases:"));
    assertThat(lines.get(5), is("  - db1"));
    assertThat(lines.get(6), is("  - db2"));
    assertThat(lines.get(7), is("  mapped-tables:"));
    assertThat(lines.get(8), is("  - database: db1"));
    assertThat(lines.get(9), is("    mapped-tables:"));
    assertThat(lines.get(10), is("    - tbl1"));
    assertThat(lines.get(11), is("  - database: db2"));
    assertThat(lines.get(12), is("    mapped-tables:"));
    assertThat(lines.get(13), is("    - tbl2"));
    assertThat(lines.get(14), is("  name: hcom_1"));
    assertThat(lines.get(15), is("  remote-meta-store-uris: thrift://localhost:19083"));
    assertThat(lines.get(16), is("federated-meta-stores:"));
    assertThat(lines.get(17), is("- access-control-type: READ_ONLY"));
    assertThat(lines.get(18), is("  database-name-mapping: {}"));
    assertThat(lines.get(19), is("  database-prefix: hcom_2_"));
    assertThat(lines.get(20), is("  latency: 0"));
    assertThat(lines.get(21), is("  name: hcom_2"));
    assertThat(lines.get(22), is("  remote-meta-store-uris: thrift://localhost:29083"));
  }

  private PrimaryMetaStore newPrimaryInstance(String name, String remoteMetaStoreUris) {
    PrimaryMetaStore result = AbstractMetaStore.newPrimaryInstance(name, remoteMetaStoreUris);
    return result;
  }

  private FederatedMetaStore newFederatedInstance(String name, String remoteMetaStoreUris) {
    FederatedMetaStore result = AbstractMetaStore.newFederatedInstance(name, remoteMetaStoreUris);
    return result;
  }

}
