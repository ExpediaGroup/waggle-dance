/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import javax.validation.ConstraintViolation;

import org.junit.Test;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PrimaryMetaStoreTest extends AbstractMetaStoreTest<PrimaryMetaStore> {

  private final String name = "name";
  private final String remoteMetaStoreUris = "remoteMetaStoreUris";
  private final List<String> whitelist = new ArrayList<>();
  private final AccessControlType accessControlType = AccessControlType.READ_AND_WRITE_ON_DATABASE_WHITELIST;

  public PrimaryMetaStoreTest() {
    super(new PrimaryMetaStore());
  }

  @Test
  public void testAccessControlTypeDefaultReadOnly() {
    assertThat(metaStore.getAccessControlType(), is(AccessControlType.READ_ONLY));

    // override
    metaStore.setAccessControlType(AccessControlType.READ_AND_WRITE_ON_DATABASE_WHITELIST);
    assertThat(metaStore.getAccessControlType(), is(AccessControlType.READ_AND_WRITE_ON_DATABASE_WHITELIST));
  }

  @Test
  public void testFederationType() {
    assertThat(metaStore.getFederationType(), is(FederationType.PRIMARY));
  }

  @Test
  public void testDefaultDatabaseWhiteListIsEmpty() {
    assertThat(metaStore.getWritableDatabaseWhiteList(), is(notNullValue()));
    assertThat(metaStore.getWritableDatabaseWhiteList().size(), is(0));
  }

  @Test
  public void emptyDatabasePrefix() {
    metaStore.setDatabasePrefix("");
    Set<ConstraintViolation<PrimaryMetaStore>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(0));
    assertThat(metaStore.getDatabasePrefix(), is(""));
  }

  @Test
  public void nullDatabasePrefix() {
    metaStore.setDatabasePrefix(null);
    Set<ConstraintViolation<PrimaryMetaStore>> violations = validator.validate(metaStore);
    // Violation is not triggered cause EMPTY STRING is always returned. Warning is logged instead
    assertThat(violations.size(), is(0));
    assertThat(metaStore.getDatabasePrefix(), is(""));
  }

  @Test
  public void nonEmptyDatabasePrefix() {
    String prefix = "abc";
    metaStore.setDatabasePrefix(prefix);
    Set<ConstraintViolation<PrimaryMetaStore>> violations = validator.validate(metaStore);
    assertThat(violations.size(), is(0));
    assertThat(metaStore.getDatabasePrefix(), is(prefix));
  }

  @Test
  public void toJson() throws Exception {
    String expected = "{\"accessControlType\":\"READ_ONLY\",\"connectionType\":\"DIRECT\",\"databasePrefix\":\"\",\"federationType\":\"PRIMARY\",\"latency\":0,\"mappedDatabases\":null,\"metastoreTunnel\":null,\"name\":\"name\",\"remoteMetaStoreUris\":\"uri\",\"status\":\"UNKNOWN\",\"writableDatabaseWhiteList\":[]}";
    ObjectMapper mapper = new ObjectMapper();
    // Sorting to get deterministic test behaviour
    mapper.enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);
    String json = mapper.writerFor(PrimaryMetaStore.class).writeValueAsString(metaStore);
    assertThat(json, is(expected));
  }

  @Test
  public void nonEmptyConstructor() {
    whitelist.add("databaseOne");
    whitelist.add("databaseTwo");
    PrimaryMetaStore store = new PrimaryMetaStore(name, remoteMetaStoreUris, accessControlType, whitelist.get(0),
        whitelist.get(1));
    assertThat(store.getName(), is(name));
    assertThat(store.getRemoteMetaStoreUris(), is(remoteMetaStoreUris));
    assertThat(store.getAccessControlType(), is(accessControlType));
    assertThat(store.getWritableDatabaseWhiteList(), is(whitelist));
  }

  @Test
  public void constructorWithArrayListForWhitelist() {
    whitelist.add("databaseOne");
    whitelist.add("databaseTwo");
    PrimaryMetaStore store = new PrimaryMetaStore(name, remoteMetaStoreUris, accessControlType, whitelist);
    assertThat(store.getName(), is(name));
    assertThat(store.getRemoteMetaStoreUris(), is(remoteMetaStoreUris));
    assertThat(store.getAccessControlType(), is(accessControlType));
    assertThat(store.getWritableDatabaseWhiteList(), is(whitelist));
  }

}
