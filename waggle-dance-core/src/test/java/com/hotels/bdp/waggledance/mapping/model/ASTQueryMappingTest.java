/**
 * Copyright (C) 2016-2025 Expedia, Inc.
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

@RunWith(MockitoJUnitRunner.class)
public class ASTQueryMappingTest {

  private static final String PREFIX = "prefix_";
  private static final long LATENCY = 0;
  private MetaStoreMapping metaStoreMapping;

  @Before
  public void setUp() {
    metaStoreMapping = new PrefixMapping(new MetaStoreMappingImpl(PREFIX, "mapping", null, null, DIRECT, LATENCY,
        new DefaultMetaStoreFilterHookImpl(new HiveConf())));
  }

  @Test
  public void transformOutboundDatabaseNamePrestoMarker() {
    ASTQueryMapping queryMapping = ASTQueryMapping.INSTANCE;

    String query = "/* Presto View */";

    assertThat(queryMapping.transformOutboundDatabaseName(metaStoreMapping, query), is(query));
  }

  @Test
  public void transformOutboundDatabaseNamePrestoExpandedTextMarker() {
    ASTQueryMapping queryMapping = ASTQueryMapping.INSTANCE;

    String query = "/* Presto View: <base64 of view sql> */";

    assertThat(queryMapping.transformOutboundDatabaseName(metaStoreMapping, query),
        is(query));
  }

  @Test
  public void transformOutboundDatabaseName() {
    ASTQueryMapping queryMapping = ASTQueryMapping.INSTANCE;

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

    String unparsableQuery = "SELCT *";
    queryMapping.transformOutboundDatabaseName(metaStoreMapping, unparsableQuery);
  }

  @Test
  public void transformOutboundDatabaseNameOnFunctions() {
    ASTQueryMapping queryMapping = ASTQueryMapping.INSTANCE;

    String query = "SELECT db1.myFunction()";

    assertThat(queryMapping.transformOutboundDatabaseName(metaStoreMapping, query),
        is("SELECT " + PREFIX + "db1.myFunction()"));
  }

  @Test
  public void transformOutboundDatabaseNameOnFunctionNoDbName() {
    ASTQueryMapping queryMapping = ASTQueryMapping.INSTANCE;

    String query = "CREATE VIEW test_view AS SELECT a.c1 FROM (SELECT fun(),1) a";
    assertThat(queryMapping.transformOutboundDatabaseName(metaStoreMapping, query),
        is("CREATE VIEW test_view AS SELECT a.c1 FROM (SELECT fun(),1) a"));

    query = "CREATE VIEW db1.test_view AS SELECT a.c1 FROM (SELECT fun(), db.fun2()) a";
    assertThat(queryMapping.transformOutboundDatabaseName(metaStoreMapping, query),
        is("CREATE VIEW " + PREFIX + "db1.test_view AS SELECT a.c1 FROM " +
                    "(SELECT fun(), " + PREFIX +"db.fun2()) a"));

    query = "SELECT hellobdp() as q union all SELECT hellobdp() as qq where false";
    assertThat(queryMapping.transformOutboundDatabaseName(metaStoreMapping, query),
        is("SELECT hellobdp() as q union all SELECT hellobdp() as qq where false"));

    query = "SELECT hellobdp() as q union all SELECT db1.hellobdp() as qq where false";
    assertThat(queryMapping.transformOutboundDatabaseName(metaStoreMapping, query),
        is("SELECT hellobdp() as q union all SELECT " + PREFIX + "db1.hellobdp() as qq where false"));

    query = "SELECT COALESCE(db1.hellobdp(`table1.id`, 1)) where false";
    assertThat(queryMapping.transformOutboundDatabaseName(metaStoreMapping, query),
        is("SELECT COALESCE(" + PREFIX + "db1.hellobdp(`table1.id`, 1)) where false"));

    query = "SELECT COALESCE(db1.hellobdp(`db2.fun2`(`table1.id`), 1)) where false";
    assertThat(queryMapping.transformOutboundDatabaseName(metaStoreMapping, query),
        is("SELECT COALESCE(" + PREFIX + "db1.hellobdp(`" + PREFIX + "db2.fun2`(`table1.id`), 1)) where false"));
  }

  @Test
  public void transformOutboundDatabaseNameOnMultipleSameFunctions() {
    ASTQueryMapping queryMapping = ASTQueryMapping.INSTANCE;

    String query = "SELECT bdp.hellobdp() as q union all SELECT bdp.hellobdp() as qq where false";

    assertThat(queryMapping.transformOutboundDatabaseName(metaStoreMapping, query),
        is("SELECT " + PREFIX + "bdp.hellobdp() as q union all SELECT " + PREFIX + "bdp.hellobdp() as qq where false"));
  }

  @Test
  public void transformOutboundDatabaseNameOnMultipleDifferentFunctions() {
    ASTQueryMapping queryMapping = ASTQueryMapping.INSTANCE;

    String query = "SELECT bdp.hellobdp1(), bdp.hellobdp2()";

    assertThat(queryMapping.transformOutboundDatabaseName(metaStoreMapping, query),
        is("SELECT " + PREFIX + "bdp.hellobdp1(), " + PREFIX + "bdp.hellobdp2()"));
  }

}
