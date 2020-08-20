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
package com.hotels.bdp.waggledance.mapping.model;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.hotels.bdp.waggledance.api.model.ConnectionType.DIRECT;

import org.junit.Test;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

@SuppressWarnings("resource")
public class DatabaseNameMappingTest {

  private final MetaStoreMapping metaStoreMapping = new MetaStoreMappingImpl("prefix", "mapping", null, null, DIRECT,
      0L);

  @Test
  public void mapNames() throws Exception {
    BiMap<String, String> databaseNameMap = HashBiMap.create();
    databaseNameMap.put("remote_name", "local_name");
    DatabaseNameMapping databaseNameMapping = new DatabaseNameMapping(metaStoreMapping, databaseNameMap);
    assertThat(databaseNameMapping.transformInboundDatabaseName("local_name"), is("remote_name"));
    assertThat(databaseNameMapping.transformOutboundDatabaseName("remote_name"), is("local_name"));
  }

  @Test
  public void noMatchingMappingReturnsOriginal() throws Exception {
    BiMap<String, String> databaseNameMap = HashBiMap.create();
    databaseNameMap.put("remote_name", "local_name");
    DatabaseNameMapping databaseNameMapping = new DatabaseNameMapping(metaStoreMapping, databaseNameMap);
    assertThat(databaseNameMapping.transformInboundDatabaseName("a"), is("a"));
    assertThat(databaseNameMapping.transformOutboundDatabaseName("a"), is("a"));
  }

  @Test
  public void nullMapping() throws Exception {
    DatabaseNameMapping databaseNameMapping = new DatabaseNameMapping(metaStoreMapping, null);
    assertThat(databaseNameMapping.transformInboundDatabaseName("a"), is("a"));
    assertThat(databaseNameMapping.transformOutboundDatabaseName("a"), is("a"));
  }

}
