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
package com.hotels.bdp.waggledance.mapping.model;

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryMappingTest {

  private static final String PREFIX = "prefix_";
  private QueryMapping queryMapping;

  @Before
  public void init() {
    MetaStoreMapping metaStoreMapping = new MetaStoreMappingImpl(PREFIX, "mapping", null, null);
    queryMapping = new QueryMapping(metaStoreMapping);
  }

  @Test
  public void transformOutboundDatabaseName() {
    String query = "SELECT *\n"
        + "FROM db1.table1 alias1 INNER JOIN db2.table2 alias2\n"
        + "ON alias1.field1 = alias2.field2";

    assertEquals("SELECT * FROM "
        + PREFIX
        + "db1.table1 alias1 JOIN "
        + PREFIX
        + "db2.table2 alias2  ON alias1.field1 = alias2.field2", queryMapping.transformOutboundDatabaseName(query));
  }
}
