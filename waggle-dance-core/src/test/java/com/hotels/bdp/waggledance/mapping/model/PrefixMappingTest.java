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
import static org.hamcrest.MatcherAssert.assertThat;

import static com.hotels.bdp.waggledance.api.model.ConnectionType.DIRECT;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.junit.Test;

@SuppressWarnings("resource")
public class PrefixMappingTest {

  private final MetaStoreMapping metaStoreMapping = new MetaStoreMappingImpl("prefix_", "mapping", null, null, DIRECT,
      0L, new DefaultMetaStoreFilterHookImpl(new HiveConf()));

  @Test
  public void mapNames() throws Exception {
    PrefixMapping prefixMapping = new PrefixMapping(metaStoreMapping);
    assertThat(prefixMapping.transformInboundDatabaseName("prefix_name"), is("name"));
    assertThat(prefixMapping.transformOutboundDatabaseName("name"), is("prefix_name"));
  }

  @Test
  public void doNotMapUnprefixedInbound() throws Exception {
    PrefixMapping prefixMapping = new PrefixMapping(metaStoreMapping);
    assertThat(prefixMapping.transformInboundDatabaseName("name"), is("name"));
    assertThat(prefixMapping.transformOutboundDatabaseName("name"), is("prefix_name"));
  }

}
