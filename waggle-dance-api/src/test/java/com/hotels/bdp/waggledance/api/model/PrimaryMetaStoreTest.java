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
package com.hotels.bdp.waggledance.api.model;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.util.Set;

import javax.validation.ConstraintViolation;

import org.junit.Test;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PrimaryMetaStoreTest extends AbstractMetaStoreTest<PrimaryMetaStore> {

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
    metaStore.setDatabasePrefix("abc");
    Set<ConstraintViolation<PrimaryMetaStore>> violations = validator.validate(metaStore);
    // Violation is not triggered cause EMPTY STRING is always returned. Warning is logged instead
    assertThat(violations.size(), is(0));
    assertThat(metaStore.getDatabasePrefix(), is(""));
  }

  @Test
  public void toJson() throws Exception {
    String expected = "{\"accessControlType\":\"READ_ONLY\",\"connectionType\":\"DIRECT_CONNECTION\",\"databasePrefix\":\"\",\"federationType\":\"PRIMARY\",\"metastoreTunnel\":null,\"name\":\"name\",\"remoteMetaStoreUris\":\"uri\",\"status\":\"UNKNOWN\",\"writableDatabaseWhiteList\":[]}";
    ObjectMapper mapper = new ObjectMapper();
    // Sorting to get deterministic test behaviour
    mapper.enable(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY);
    String json = mapper.writerFor(PrimaryMetaStore.class).writeValueAsString(metaStore);
    assertThat(json, is(expected));
  }
}
