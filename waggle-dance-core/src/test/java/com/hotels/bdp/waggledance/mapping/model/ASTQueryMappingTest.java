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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import static com.hotels.bdp.waggledance.api.model.ConnectionType.DIRECT;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

@RunWith(MockitoJUnitRunner.class)
public class ASTQueryMappingTest {

  private static final String PREFIX = "prefix_";

  @Test
  public void transformOutboundDatabaseName() {
    ASTQueryMapping queryMapping = ASTQueryMapping.INSTANCE;
    MetaStoreMapping metaStoreMapping = new MetaStoreMappingImpl(PREFIX, "mapping", null, null, DIRECT);

    String query = "SELECT *\n"
        + "FROM db1.table1 alias1 INNER JOIN db2.table2 alias2\n"
        + "ON alias1.field1 = alias2.field2";

    assertThat(queryMapping.transformOutboundDatabaseName(metaStoreMapping, query),
        is("SELECT *\n"
            + "FROM "
            + PREFIX
            + "db1.table1 alias1 INNER JOIN "
            + PREFIX
            + "db2.table2 alias2\n"
            + "ON alias1.field1 = alias2.field2"));
  }

  @Test
  public void transformOutboundDatabaseNameAliasWithBackTicks() {
    ASTQueryMapping queryMapping = ASTQueryMapping.INSTANCE;
    MetaStoreMapping metaStoreMapping = new MetaStoreMappingImpl(PREFIX, "mapping", null, null, DIRECT);

    String query = "";
    query += "SELECT col_id AS id ";
    query += "FROM (SELECT `table1`.id";
    query += " FROM `db1`.`table1`) `db1.table1`";

    String expected = "";
    expected += "SELECT col_id AS id ";
    expected += "FROM (SELECT `table1`.id";
    expected += " FROM `" + PREFIX + "db1`.`table1`) `db1.table1`";

    assertThat(queryMapping.transformOutboundDatabaseName(metaStoreMapping, query), is(expected));
  }

  @Test(expected = WaggleDanceException.class)
  public void transformOutboundDatabaseNameParseException() {
    ASTQueryMapping queryMapping = ASTQueryMapping.INSTANCE;
    MetaStoreMapping metaStoreMapping = new MetaStoreMappingImpl(PREFIX, "mapping", null, null, DIRECT);

    String unparsableQuery = "SELCT *";
    queryMapping.transformOutboundDatabaseName(metaStoreMapping, unparsableQuery);
  }
}
