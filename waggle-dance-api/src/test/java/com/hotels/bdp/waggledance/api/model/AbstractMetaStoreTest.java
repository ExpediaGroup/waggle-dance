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
package com.hotels.bdp.waggledance.api.model;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.validation.ConstraintViolation;

import org.hibernate.validator.HibernateValidator;
import org.junit.Before;
import org.junit.Test;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;

import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;

public abstract class AbstractMetaStoreTest<T extends AbstractMetaStore> {

  final LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();

  final T metaStore;

  private final String name = "name";
  private final String remoteMetaStoreUri = "uri";

  AbstractMetaStoreTest(T metaStore) {
    this.metaStore = metaStore;
  }

  private static MetastoreTunnel newMetastoreTunnel() {
    MetastoreTunnel metastoreTunnel = new MetastoreTunnel();
    metastoreTunnel.setRoute("user@jumpbox -> host");
    metastoreTunnel.setPrivateKeys("privateKeys");
    metastoreTunnel.setKnownHosts("knownHosts");
    return metastoreTunnel;
  }

  @Before
  public void before() {
    validator.setProviderClass(HibernateValidator.class);
    validator.afterPropertiesSet();

    metaStore.setRemoteMetaStoreUris(remoteMetaStoreUri);
    metaStore.setName(name);
  }

  @Test
  public void typical() {
    Set<ConstraintViolation<T>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void validMetastoreTunnel() {
    metaStore.setMetastoreTunnel(newMetastoreTunnel());
    Set<ConstraintViolation<T>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void invalidMetastoreTunnel() {
    MetastoreTunnel metastoreTunnel = newMetastoreTunnel();
    metastoreTunnel.setPort(-1);
    metaStore.setMetastoreTunnel(metastoreTunnel);

    Set<ConstraintViolation<T>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullName() {
    metaStore.setName(null);
    Set<ConstraintViolation<T>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void emptyName() {
    metaStore.setName(" ");
    Set<ConstraintViolation<T>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void equalsNull() {
    assertNotEquals(metaStore, null);
  }

  @Test
  public void equalsDifferentClass() {
    assertNotEquals("string", metaStore);
  }

  @Test
  public void newFederatedInstance() {
    FederatedMetaStore federatedMetaStore = AbstractMetaStore.newFederatedInstance(name, remoteMetaStoreUri);
    assertThat(federatedMetaStore.getName(), is(name));
    assertThat(federatedMetaStore.getRemoteMetaStoreUris(), is(remoteMetaStoreUri));
  }

  @Test
  public void newPrimaryInstance() {
    AccessControlType access = AccessControlType.READ_AND_WRITE_AND_CREATE;
    PrimaryMetaStore primaryMetaStore = AbstractMetaStore.newPrimaryInstance(name, remoteMetaStoreUri, access);
    assertThat(primaryMetaStore.getName(), is(name));
    assertThat(primaryMetaStore.getRemoteMetaStoreUris(), is(remoteMetaStoreUri));
    assertThat(primaryMetaStore.getAccessControlType(), is(access));
  }

  @Test
  public void newPrimaryInstanceWithDefaultAccessControlType() {
    PrimaryMetaStore primaryMetaStore = AbstractMetaStore.newPrimaryInstance(name, remoteMetaStoreUri);
    assertThat(primaryMetaStore.getName(), is(name));
    assertThat(primaryMetaStore.getRemoteMetaStoreUris(), is(remoteMetaStoreUri));
    assertThat(primaryMetaStore.getAccessControlType(), is(AccessControlType.READ_ONLY));
  }

  @Test
  public void mappedDatabases() {
    List<String> mappedDatabases = new ArrayList<>();
    mappedDatabases.add("database");
    metaStore.setMappedDatabases(mappedDatabases);
    assertThat(metaStore.getMappedDatabases(), is(mappedDatabases));
  }

  @Test
  public void nullMappedDatabases() {
    metaStore.setMappedDatabases(null);
    assertThat(metaStore.getMappedDatabases(), is(nullValue()));
  }

  @Test
  public void emptyMappedDatabases() {
    metaStore.setMappedDatabases(Collections.emptyList());
    assertThat(metaStore.getMappedDatabases().size(), is(0));
  }

  @Test
  public void mappedTables() {
    MappedTables mappedTables1 = new MappedTables("db1", Lists.newArrayList("tbl1"));
    MappedTables mappedTables2 = new MappedTables("db2", Lists.newArrayList("tbl2"));
    List<MappedTables> mappedTables = Lists.newArrayList(mappedTables1, mappedTables2);
    metaStore.setMappedTables(mappedTables);
    assertThat(metaStore.getMappedTables(), is(mappedTables));

    Set<ConstraintViolation<T>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void mappedTablesEmptyDbInvalid() {
    MappedTables mappedTables = new MappedTables("", Lists.newArrayList("tbl1"));
    metaStore.setMappedTables(Lists.newArrayList(mappedTables));

    Set<ConstraintViolation<T>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void mappedTablesNullDbInvalid() {
    MappedTables mappedTables = new MappedTables(null, Lists.newArrayList("tbl1"));
    metaStore.setMappedTables(Lists.newArrayList(mappedTables));

    Set<ConstraintViolation<T>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void mappedTablesNullTblInvalid() {
    MappedTables mappedTables = new MappedTables("valid_db", null);
    metaStore.setMappedTables(Lists.newArrayList(mappedTables));

    Set<ConstraintViolation<T>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void mappedTablesEmptyTblsInvalid() {
    MappedTables mappedTables = new MappedTables("valid_db", Lists.newArrayList());
    metaStore.setMappedTables(Lists.newArrayList(mappedTables));

    Set<ConstraintViolation<T>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullMappedTables() {
    metaStore.setMappedTables(null);
    assertThat(metaStore.getMappedTables(), is(nullValue()));

    Set<ConstraintViolation<T>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void emptyMappedTables() {
    metaStore.setMappedTables(Collections.emptyList());
    assertThat(metaStore.getMappedTables().size(), is(0));

    Set<ConstraintViolation<T>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void setDatabasesNameMapping() throws Exception {
    Map<String, String> mapping = new HashMap<>();
    mapping.put("a", "b");
    metaStore.setDatabaseNameMapping(mapping);
    assertThat(metaStore.getDatabaseNameMapping(), is(mapping));
    assertThat(metaStore.getDatabaseNameBiMapping(), is(HashBiMap.create(mapping)));
  }

  @Test
  public void setDatabasesNameMappingNullToEmpty() throws Exception {
    metaStore.setDatabaseNameMapping(null);
    assertThat(metaStore.getDatabaseNameMapping().size(), is(0));
    assertThat(metaStore.getDatabaseNameBiMapping().size(), is(0));
  }

}
