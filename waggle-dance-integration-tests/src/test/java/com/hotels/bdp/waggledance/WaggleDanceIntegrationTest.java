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
package com.hotels.bdp.waggledance;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import static com.google.common.collect.Lists.newArrayList;

import static com.hotels.bdp.waggledance.TestUtils.createPartitionedTable;
import static com.hotels.bdp.waggledance.TestUtils.createUnpartitionedTable;
import static com.hotels.bdp.waggledance.TestUtils.newPartition;
import static com.hotels.bdp.waggledance.api.model.AccessControlType.READ_AND_WRITE_ON_DATABASE_WHITELIST;
import static com.hotels.bdp.waggledance.api.model.AccessControlType.READ_ONLY;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.web.client.RestTemplate;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import fm.last.commons.test.file.ClassDataFolder;
import fm.last.commons.test.file.DataFolder;

import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.api.model.AccessControlType;
import com.hotels.bdp.waggledance.api.model.DatabaseResolution;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.Federations;
import com.hotels.bdp.waggledance.api.model.MappedTables;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;
import com.hotels.bdp.waggledance.junit.ServerSocketRule;
import com.hotels.bdp.waggledance.mapping.model.PrefixingMetastoreFilter;
import com.hotels.bdp.waggledance.yaml.YamlFactory;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;

public class WaggleDanceIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(WaggleDanceIntegrationTest.class);

  private static final String LOCAL_DATABASE = "local_database";
  private static final String LOCAL_TABLE = "local_table";
  private static final String REMOTE_DATABASE = "remote_database";
  private static final String REMOTE_TABLE = "remote_table";
  private static final String SECONDARY_METASTORE_NAME = "waggle_remote";
  private static final String PREFIXED_REMOTE_DATABASE = SECONDARY_METASTORE_NAME + "_" + REMOTE_DATABASE;

  public @Rule ServerSocketRule graphite = new ServerSocketRule();
  public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
  public @Rule ThriftHiveMetaStoreJUnitRule localServer = new ThriftHiveMetaStoreJUnitRule(LOCAL_DATABASE);
  public @Rule ThriftHiveMetaStoreJUnitRule remoteServer = new ThriftHiveMetaStoreJUnitRule(REMOTE_DATABASE);
  public @Rule ThriftHiveMetaStoreJUnitRule newRemoteServer = new ThriftHiveMetaStoreJUnitRule();
  public @Rule DataFolder dataFolder = new ClassDataFolder();

  private WaggleDanceRunner runner;

  private File configLocation;
  private File localWarehouseUri;
  private File remoteWarehouseUri;

  @Before
  public void init() throws Exception {
    configLocation = temporaryFolder.newFolder("config");
    localWarehouseUri = temporaryFolder.newFolder("local-warehouse");
    remoteWarehouseUri = temporaryFolder.newFolder("remote-warehouse");

    createLocalTable(new File(localWarehouseUri, LOCAL_DATABASE + "/" + LOCAL_TABLE), LOCAL_TABLE);
    LOG.info(">>>> Table {} ", localServer.client().getTable(LOCAL_DATABASE, LOCAL_TABLE));

    createRemoteTable(new File(remoteWarehouseUri, REMOTE_DATABASE + "/" + REMOTE_TABLE), REMOTE_TABLE);
    LOG.info(">>>> Table {} ", remoteServer.client().getTable(REMOTE_DATABASE, REMOTE_TABLE));
  }

  @After
  public void destroy() throws Exception {
    if (runner != null) {
      runner.stop();
    }
  }

  private void createLocalTable(File tableUri, String table) throws Exception {
    createUnpartitionedTable(localServer.client(), LOCAL_DATABASE, table, tableUri);
  }

  private void createRemoteTable(File tableUri, String table) throws Exception {
    HiveMetaStoreClient client = remoteServer.client();

    Table hiveTable = createPartitionedTable(client, REMOTE_DATABASE, table, tableUri);

    File partitionEurope = new File(tableUri, "continent=Europe");
    File partitionUk = new File(partitionEurope, "country=UK");

    File partitionAsia = new File(tableUri, "continent=Asia");
    File partitionChina = new File(partitionAsia, "country=China");

    LOG
        .info(">>>> Partitions added: {}",
            client
                .add_partitions(Arrays
                    .asList(newPartition(hiveTable, Arrays.asList("Europe", "UK"), partitionUk),
                        newPartition(hiveTable, Arrays.asList("Asia", "China"), partitionChina))));
  }

  private void runWaggleDance(WaggleDanceRunner runner) throws Exception {
    runner.runAndWaitForStartup();
  }

  private Federations stopServerAndGetConfiguration() throws Exception, FileNotFoundException {
    runner.stop();
    // Stopping the server triggers the saving of the config file.
    Federations federations = YamlFactory
        .newYaml()
        .loadAs(new FileInputStream(runner.federationConfig()), Federations.class);
    return federations;
  }

  @Test
  public void typical() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    // Local table
    Table localTable = localServer.client().getTable(LOCAL_DATABASE, LOCAL_TABLE);
    Table waggledLocalTable = proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
    assertThat(waggledLocalTable, is(localTable));

    // Remote table
    String waggledRemoteDbName = REMOTE_DATABASE;
    assertTypicalRemoteTable(proxy, waggledRemoteDbName);
  }

  @Test
  public void typicalGetAllFunctions() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .withPrimaryPrefix("primary_")
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();
    List<ResourceUri> resourceUris = Lists
        .newArrayList(new ResourceUri(ResourceType.JAR, "hdfs://path/to/my/jar/my.jar"));
    Function localFunction = new Function("fn1", LOCAL_DATABASE, "com.hotels.hive.FN1", "hadoop", PrincipalType.USER, 0,
        FunctionType.JAVA, resourceUris);
    localServer.client().createFunction(localFunction);
    Function remoteFunction = new Function("fn2", REMOTE_DATABASE, "com.hotels.hive.FN1", "hadoop", PrincipalType.USER,
        0, FunctionType.JAVA, resourceUris);
    remoteServer.client().createFunction(remoteFunction);

    GetAllFunctionsResponse allFunctions = proxy.getAllFunctions();
    List<Function> functions = allFunctions.getFunctions();
    assertThat(functions.size(), is(3));
    assertThat(functions.get(0).getFunctionName(), is("fn1"));
    assertThat(functions.get(0).getDbName(), is("primary_" + LOCAL_DATABASE));
    assertThat(functions.get(1).getFunctionName(), is("fn1"));
    assertThat(functions.get(1).getDbName(), is(LOCAL_DATABASE));
    assertThat(functions.get(2).getFunctionName(), is("fn2"));
    assertThat(functions.get(2).getDbName(), is(PREFIXED_REMOTE_DATABASE));
  }

  @Test
  public void usePrefix() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri())
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();
    HiveMetaStoreClient proxy2 = runner.createWaggleDanceClient();

    // Local table
    Table localTable = localServer.client().getTable(LOCAL_DATABASE, LOCAL_TABLE);
    Table waggledLocalTable = proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
    assertThat(waggledLocalTable, is(localTable));
    waggledLocalTable = proxy2.getTable(LOCAL_DATABASE, LOCAL_TABLE);
    assertThat(waggledLocalTable, is(localTable));

    // Remote table
    String waggledRemoteDbName = PREFIXED_REMOTE_DATABASE;
    assertTypicalRemoteTable(proxy, waggledRemoteDbName);
  }

  @Test
  public void manyFederatedMetastores() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri())
        .federate("fed1", remoteServer.getThriftConnectionUri())
        .federate("fed2", remoteServer.getThriftConnectionUri())
        .federate("fed3", remoteServer.getThriftConnectionUri())
        .federate("fed4", remoteServer.getThriftConnectionUri())
        .federate("fed5", remoteServer.getThriftConnectionUri())
        .federate("fed6", remoteServer.getThriftConnectionUri())
        .federate("fed7", remoteServer.getThriftConnectionUri())
        .federate("fed8", remoteServer.getThriftConnectionUri())
        .federate("fed9", remoteServer.getThriftConnectionUri())
        .federate("fed10", remoteServer.getThriftConnectionUri())
        .federate("fed11", remoteServer.getThriftConnectionUri())
        .federate("fed12", remoteServer.getThriftConnectionUri())
        .federate("fed13", remoteServer.getThriftConnectionUri())
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    List<String> dbs = proxy.getAllDatabases();
    List<String> expected = newArrayList("default", "local_database", "waggle_remote_default",
        "waggle_remote_remote_database", "fed1_default", "fed1_remote_database", "fed2_default", "fed2_remote_database",
        "fed3_default", "fed3_remote_database", "fed4_default", "fed4_remote_database", "fed5_default",
        "fed5_remote_database", "fed6_default", "fed6_remote_database", "fed7_default", "fed7_remote_database",
        "fed8_default", "fed8_remote_database", "fed9_default", "fed9_remote_database", "fed10_default",
        "fed10_remote_database", "fed11_default", "fed11_remote_database", "fed12_default", "fed12_remote_database",
        "fed13_default", "fed13_remote_database");
    assertThat(dbs, is(expected));
  }

  @Test
  public void usePrimaryPrefix() throws Exception {
    String primaryPrefix = "primary_";
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .withPrimaryPrefix(primaryPrefix)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri())
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    // Local table
    String prefixedLocalDbName = primaryPrefix + LOCAL_DATABASE;
    Table localTable = proxy.getTable(prefixedLocalDbName, LOCAL_TABLE);
    assertThat(localTable.getDbName(), is(prefixedLocalDbName));

    // fetch without prefix works and result is prefixed
    Table localTable2 = proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
    assertThat(localTable2.getDbName(), is(prefixedLocalDbName));

    // Remote table
    String prefixedRemoteDbName = PREFIXED_REMOTE_DATABASE;
    assertTypicalRemoteTable(proxy, prefixedRemoteDbName);
  }

  private void assertTypicalRemoteTable(HiveMetaStoreClient proxy, String waggledRemoteDbName) throws TException {
    Table remoteTable = remoteServer.client().getTable(REMOTE_DATABASE, REMOTE_TABLE);
    Table waggledRemoteTable = proxy.getTable(waggledRemoteDbName, REMOTE_TABLE);
    assertThat(waggledRemoteTable.getDbName(), is(waggledRemoteDbName));
    assertThat(waggledRemoteTable.getTableName(), is(remoteTable.getTableName()));
    assertThat(waggledRemoteTable.getSd(), is(remoteTable.getSd()));
    assertThat(waggledRemoteTable.getParameters(), is(remoteTable.getParameters()));
    assertThat(waggledRemoteTable.getPartitionKeys(), is(remoteTable.getPartitionKeys()));

    List<String> partitionNames = Arrays.asList("continent=Europe/country=UK", "continent=Asia/country=China");

    List<Partition> remotePartitions = remoteServer
        .client()
        .getPartitionsByNames(REMOTE_DATABASE, REMOTE_TABLE, partitionNames);
    List<Partition> waggledRemotePartitions = proxy
        .getPartitionsByNames(waggledRemoteDbName, REMOTE_TABLE, partitionNames);
    assertThat(waggledRemotePartitions.size(), is(2));
    for (int i = 0; i < waggledRemotePartitions.size(); ++i) {
      Partition remotePartition = remotePartitions.get(i);
      Partition waggledRemotePartition = waggledRemotePartitions.get(i);
      assertThat(remotePartition.getDbName(), is(REMOTE_DATABASE));
      assertThat(waggledRemotePartition.getDbName(), is(waggledRemoteDbName));
      assertThat(waggledRemotePartition.getTableName(), is(remotePartition.getTableName()));
      assertThat(waggledRemotePartition.getCreateTime(), is(remotePartition.getCreateTime()));
      assertThat(waggledRemotePartition.getParameters(), is(remotePartition.getParameters()));
      assertThat(waggledRemotePartition.getPrivileges(), is(remotePartition.getPrivileges()));
      assertThat(waggledRemotePartition.getSd(), is(remotePartition.getSd()));
      assertThat(waggledRemotePartition.getValues(), is(remotePartition.getValues()));
    }
  }

  @Ignore("Seems to fail for unknown reasons often in Github Actions")
  @Test
  public void typicalWithGraphite() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .federate("remote", remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .graphite("localhost", graphite.port(), "graphitePrefix", 1000)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    // Execute a couple of requests
    proxy.getAllDatabases();
    proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
    proxy.getAllDatabases();
    proxy.getTable(REMOTE_DATABASE, REMOTE_TABLE);
    runner.stop();

    Set<String> metrics = new TreeSet<>(Arrays.asList(new String(graphite.getOutput()).split("\n")));
    assertMetric(metrics,
        "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_all_databases.all.calls;metricattribute=count 2");
    assertMetric(metrics,
        "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_all_databases.all.success;metricattribute=count 2");
    assertMetric(metrics,
        "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_table_req.primary.calls;metricattribute=count 1");
    assertMetric(metrics,
        "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_table_req.primary.success;metricattribute=count 1");
    assertMetric(metrics,
        "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_table_req.remote.calls;metricattribute=count 1");
    assertMetric(metrics,
        "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_table_req.remote.success;metricattribute=count 1");
    assertMetric(metrics, "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.success");
    assertMetric(metrics, "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.calls");
  }

  private void assertMetric(Set<String> metrics, String partialMetric) {
    for (String metric : metrics) {
      if (metric.startsWith(partialMetric)) {
        return;
      }
    }
    fail(String.format("Metric '%s' not found", partialMetric));
  }

  @Test
  public void readWriteCreateAllowed() throws Exception {
    String writableDatabase = "writable_db";

    localServer.createDatabase(writableDatabase);

    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(), AccessControlType.READ_AND_WRITE_AND_CREATE)
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();
    // create rights
    proxy.createDatabase(new Database("newDB", "", new File(localWarehouseUri, "newDB").toURI().toString(), null));
    Database newDB = proxy.getDatabase("newDB");
    assertNotNull(newDB);

    // read rights
    Table localTable = localServer.client().getTable(LOCAL_DATABASE, LOCAL_TABLE);
    Table waggledLocalTable = proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
    assertThat(waggledLocalTable, is(localTable));

    // write rights
    proxy.dropTable(LOCAL_DATABASE, LOCAL_TABLE);
    try {
      proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
      fail("Should get NoSuchObjectException");
    } catch (NoSuchObjectException e) {
      // Local table should be allowed to drop, so it now no longer exists and we get an appropriate exception
    }
  }

  @Test
  public void readWriteCreateAllowedPrefixed() throws Exception {
    String writableDatabase = "writable_db";

    localServer.createDatabase(writableDatabase);

    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(), AccessControlType.READ_AND_WRITE_AND_CREATE)
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();
    // create rights
    proxy.createDatabase(new Database("newDB", "", new File(localWarehouseUri, "newDB").toURI().toString(), null));
    Database newDB = proxy.getDatabase("newDB");
    assertNotNull(newDB);

    // read rights
    Table localTable = localServer.client().getTable(LOCAL_DATABASE, LOCAL_TABLE);
    Table waggledLocalTable = proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
    assertThat(waggledLocalTable, is(localTable));

    // write rights
    proxy.dropTable(LOCAL_DATABASE, LOCAL_TABLE);
    try {
      proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
      fail("Should get NoSuchObjectException");
    } catch (NoSuchObjectException e) {
      // Local table should be allowed to drop, so it now no longer exists and we get an appropriate exception
    }
  }

  @Test
  public void federatedWritesSucceedIfReadAndWriteOnDatabaseWhiteListIsConfigured() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(), AccessControlType.READ_ONLY)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), READ_AND_WRITE_ON_DATABASE_WHITELIST,
            new String[] { REMOTE_DATABASE }, new String[] { REMOTE_DATABASE })
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    String waggledRemoteDbName = PREFIXED_REMOTE_DATABASE;

    assertTypicalRemoteTable(proxy, waggledRemoteDbName);

    // get succeeds
    proxy.getTable(waggledRemoteDbName, REMOTE_TABLE);
    // drop table
    proxy.dropTable(waggledRemoteDbName, REMOTE_TABLE);
    try {
      // get fails
      proxy.getTable(waggledRemoteDbName, REMOTE_TABLE);
      fail("Should get NoSuchObjectException");
    } catch (NoSuchObjectException e) {
      // Federated table should be allowed to drop, so it now no longer exists and we get an appropriate exception
    }
  }

  @Test
  public void federatedWritesFailIfReadAndWriteOnDatabaseWhiteListIsNotConfigured() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(), AccessControlType.READ_ONLY)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri())
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    String waggledRemoteDbName = PREFIXED_REMOTE_DATABASE;

    assertTypicalRemoteTable(proxy, waggledRemoteDbName);

    // get succeeds
    proxy.getTable(waggledRemoteDbName, REMOTE_TABLE);

    try {
      // drop fails
      proxy.dropTable(waggledRemoteDbName, REMOTE_TABLE);
      fail("Should get MetaException");
    } catch (MetaException e) {
      // Federated table should not be allowed to drop
    }
  }

  @Test
  public void federatedWritesFailIfReadAndWriteOnDatabaseWhiteListDoesNotIncludeDb() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(), AccessControlType.READ_ONLY)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), READ_AND_WRITE_ON_DATABASE_WHITELIST,
            new String[] { REMOTE_DATABASE }, new String[] { "mismatch" })
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    String waggledRemoteDbName = PREFIXED_REMOTE_DATABASE;

    assertTypicalRemoteTable(proxy, waggledRemoteDbName);

    // get succeeds
    proxy.getTable(waggledRemoteDbName, REMOTE_TABLE);

    try {
      // drop fails
      proxy.dropTable(waggledRemoteDbName, REMOTE_TABLE);
      fail("Should get MetaException");
    } catch (MetaException e) {
      // Federated table should not be allowed to drop
    }
  }

  @Test
  public void databaseWhitelisting() throws Exception {
    String writableDatabase = "writable_db";

    localServer.createDatabase(writableDatabase);

    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(), READ_AND_WRITE_ON_DATABASE_WHITELIST,
            writableDatabase)
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();
    Database readOnlyDB = proxy.getDatabase(LOCAL_DATABASE);
    assertNotNull(readOnlyDB);
    Database writableDB = proxy.getDatabase(writableDatabase);
    assertNotNull(writableDB);
    try {
      proxy.alterDatabase(LOCAL_DATABASE, new Database(readOnlyDB));
      fail("Alter DB should not be allowed");
    } catch (MetaException e) {
      // expected
      proxy.reconnect();
    }
    proxy.alterDatabase(writableDatabase, new Database(writableDB));
  }

  @Test
  public void createDatabaseUsingManualAndWhitelistingUpdatesConfig() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();
    proxy.createDatabase(new Database("newDB", "", new File(localWarehouseUri, "newDB").toURI().toString(), null));
    Database newDB = proxy.getDatabase("newDB");
    assertNotNull(newDB);

    // Should be allowed due to newly loaded write privileges
    proxy.alterDatabase("newDB", new Database(newDB));

    Federations federations = stopServerAndGetConfiguration();
    PrimaryMetaStore metaStore = federations.getPrimaryMetaStore();
    assertThat(metaStore.getWritableDatabaseWhiteList().size(), is(1));
    assertThat(metaStore.getWritableDatabaseWhiteList().get(0), is("newdb"));
    // Mapped databases should still map everything
    assertThat(metaStore.getMappedDatabases(), is(nullValue()));
  }

  @Test
  public void createDatabaseDatabaseUsingPrefixAndWhitelistingUpdates() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();
    proxy.createDatabase(new Database("newDB", "", new File(localWarehouseUri, "newDB").toURI().toString(), null));
    Database newDB = proxy.getDatabase("newDB");
    assertNotNull(newDB);

    // Should be allowed due to newly loaded write privileges
    proxy.alterDatabase("newDB", new Database(newDB));

    Federations federations = stopServerAndGetConfiguration();
    PrimaryMetaStore metaStore = federations.getPrimaryMetaStore();
    assertThat(metaStore.getWritableDatabaseWhiteList().size(), is(1));
    assertThat(metaStore.getWritableDatabaseWhiteList().get(0), is("newdb"));
    // Mapped databases should still map everything
    assertThat(metaStore.getMappedDatabases(), is(nullValue()));
  }

  @Test
  public void alterTableOnFederatedIsNotAllowedUsingManual() throws Exception {
    String[] mappableDatabases = new String[] { REMOTE_DATABASE };
    String[] writableDatabaseWhitelist = new String[] { REMOTE_DATABASE };
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.MANUAL)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .federate("doesNotMatterManualMode", remoteServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_ON_DATABASE_WHITELIST, mappableDatabases, writableDatabaseWhitelist)
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();
    Table table = proxy.getTable(REMOTE_DATABASE, REMOTE_TABLE);
    Table newTable = new Table(table);
    newTable.setTableName("new_remote_table");

    proxy.alter_table_with_environmentContext(REMOTE_DATABASE, REMOTE_TABLE, newTable, null);
    assertThat(proxy.tableExists(REMOTE_DATABASE, "new_remote_table"), is(true));
  }

  @Test
  public void doesNotOverwriteConfigOnShutdownManualMode() throws Exception {
    // Note a similar test for PREFIX is not required
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.MANUAL)
        .overwriteConfigOnShutdown(false)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();
    proxy.createDatabase(new Database("newDB", "", new File(localWarehouseUri, "newDB").toURI().toString(), null));
    Database newDB = proxy.getDatabase("newDB");
    assertNotNull(newDB);

    Federations federations = stopServerAndGetConfiguration();

    PrimaryMetaStore primaryMetastore = federations.getPrimaryMetaStore();
    List<String> writableDatabases = primaryMetastore.getWritableDatabaseWhiteList();
    assertThat(writableDatabases.size(), is(0));

    // Double check federated metastores
    List<FederatedMetaStore> federatedMetastores = federations.getFederatedMetaStores();
    assertThat(federatedMetastores.size(), is(1));

    List<String> mappedDatabases = federatedMetastores.get(0).getMappedDatabases();
    assertThat(mappedDatabases.size(), is(1));
    assertThat(mappedDatabases.get(0), is(REMOTE_DATABASE));
  }

  @Test
  public void overwritesConfigOnShutdownAfterAddingFederation() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    FederationsAdminClient restClient = Feign
        .builder()
        .contract(new JAXRSContract())
        .encoder(new JacksonEncoder())
        .decoder(new JacksonDecoder())
        .target(FederationsAdminClient.class, "http://localhost:" + runner.getRestApiPort() + "/");

    FederatedMetaStore newFederation = new FederatedMetaStore("new_waggle_remote",
        newRemoteServer.getThriftConnectionUri());
    restClient.add(newFederation);

    Federations federations = stopServerAndGetConfiguration();

    List<FederatedMetaStore> federatedMetastores = federations.getFederatedMetaStores();
    assertThat(federatedMetastores.size(), is(2));

    FederatedMetaStore remoteMetastore = federatedMetastores.get(0);
    assertThat(remoteMetastore.getName(), is(SECONDARY_METASTORE_NAME));
    assertThat(remoteMetastore.getMappedDatabases().size(), is(1));
    assertThat(remoteMetastore.getMappedDatabases().get(0), is(REMOTE_DATABASE));

    FederatedMetaStore newRemoteMetastore = federatedMetastores.get(1);
    assertThat(newRemoteMetastore.getName(), is("new_waggle_remote"));
    assertThat(newRemoteMetastore.getMappedDatabases(), is(nullValue()));
  }

  @Test
  public void doesNotOverwriteConfigOnShutdownAfterAddingFederation() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .overwriteConfigOnShutdown(false)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    FederationsAdminClient restClient = Feign
        .builder()
        .contract(new JAXRSContract())
        .encoder(new JacksonEncoder())
        .decoder(new JacksonDecoder())
        .target(FederationsAdminClient.class, "http://localhost:" + runner.getRestApiPort() + "/");

    FederatedMetaStore newFederation = new FederatedMetaStore("new_waggle_remote",
        newRemoteServer.getThriftConnectionUri());
    restClient.add(newFederation);

    Federations federations = stopServerAndGetConfiguration();

    List<FederatedMetaStore> federatedMetastores = federations.getFederatedMetaStores();
    assertThat(federatedMetastores.size(), is(1));

    FederatedMetaStore remoteMetastore = federatedMetastores.get(0);
    assertThat(remoteMetastore.getName(), is(SECONDARY_METASTORE_NAME));
    assertThat(remoteMetastore.getMappedDatabases().size(), is(1));
    assertThat(remoteMetastore.getMappedDatabases().get(0), is(REMOTE_DATABASE));
  }

  @Test
  public void restApiGetStatus() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);

    RestTemplate rest = new RestTemplateBuilder().build();
    PrimaryMetaStore primaryMetastore = rest
        .getForObject("http://localhost:" + runner.getRestApiPort() + "/api/admin/federations/primary",
            PrimaryMetaStore.class);
    assertThat(primaryMetastore.getStatus(), is(MetaStoreStatus.AVAILABLE));
    FederatedMetaStore federatedMetastore = rest
        .getForObject(
            "http://localhost:" + runner.getRestApiPort() + "/api/admin/federations/" + SECONDARY_METASTORE_NAME,
            FederatedMetaStore.class);
    assertThat(federatedMetastore.getStatus(), is(MetaStoreStatus.AVAILABLE));
  }

  // This does not set up a tunnel, but tests if a configuration with metastore-tunnel can be read correctly
  @Test
  public void metastoreTunnelConfiguration() throws Exception {
    String route = "ec2-user@bastion-host -> hadoop@emr-master";
    String privateKeys = "/home/user/.ssh/bastion-key-pair.pem,/home/user/.ssh/emr-key-pair.pem";
    String knownHosts = "/home/user/.ssh/known_hosts";
    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .federateWithMetastoreTunnel(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE,
            route, privateKeys, knownHosts)
        .build();

    runWaggleDance(runner);
    RestTemplate rest = new RestTemplateBuilder().build();
    FederatedMetaStore federatedMetastore = rest
        .getForObject(
            "http://localhost:" + runner.getRestApiPort() + "/api/admin/federations/" + SECONDARY_METASTORE_NAME,
            FederatedMetaStore.class);

    MetastoreTunnel metastoreTunnel = federatedMetastore.getMetastoreTunnel();
    assertThat(metastoreTunnel.getRoute(), is(route));
    assertThat(metastoreTunnel.getKnownHosts(), is(knownHosts));
    assertThat(metastoreTunnel.getPrivateKeys(), is(privateKeys));
  }

  @Test
  public void getDatabaseFromPatternManual() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.MANUAL)
        .overwriteConfigOnShutdown(false)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), "remote.?database")
        .federate("third", remoteServer.getThriftConnectionUri(), "no_match.*")
        .build();
    runWaggleDance(runner);

    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();
    List<String> allDatabases = proxy.getAllDatabases();

    List<String> expected = Lists.newArrayList("default", LOCAL_DATABASE, REMOTE_DATABASE);
    assertThat(allDatabases, is(expected));
  }

  @Test
  public void getDatabaseFromPatternPrefixed() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .overwriteConfigOnShutdown(false)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), "remote.?database")
        .federate("third", remoteServer.getThriftConnectionUri(), "no_match.*")
        .build();
    runWaggleDance(runner);

    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();
    List<String> allDatabases = proxy.getAllDatabases();

    List<String> expected = Lists.newArrayList("default", LOCAL_DATABASE, PREFIXED_REMOTE_DATABASE);
    assertThat(allDatabases, is(expected));
  }

  @Test
  public void primaryMappedDatabasesManual() throws Exception {
    localServer.createDatabase("random_primary");
    remoteServer.createDatabase("random_federated");

    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.MANUAL)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .withPrimaryMappedDatabases(new String[] { LOCAL_DATABASE })
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    List<String> allDatabases = proxy.getAllDatabases();
    assertThat(allDatabases.size(), is(2));
    assertThat(allDatabases.get(0), is(LOCAL_DATABASE));
    assertThat(allDatabases.get(1), is(REMOTE_DATABASE));

    // Ensure that the database is added to mapped-databases
    proxy.createDatabase(new Database("newDB", "", new File(localWarehouseUri, "newDB").toURI().toString(), null));
    Federations federations = stopServerAndGetConfiguration();
    PrimaryMetaStore primaryMetaStore = federations.getPrimaryMetaStore();
    assertThat(primaryMetaStore.getMappedDatabases().contains("newdb"), is(true));
  }

  @Test
  public void primaryMappedDatabasesPrefixed() throws Exception {
    localServer.createDatabase("random_primary");
    remoteServer.createDatabase("random_federated");

    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .withPrimaryMappedDatabases(new String[] { LOCAL_DATABASE })
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    List<String> allDatabases = proxy.getAllDatabases();
    assertThat(allDatabases.size(), is(2));
    assertThat(allDatabases.get(0), is(LOCAL_DATABASE));
    assertThat(allDatabases.get(1), is(PREFIXED_REMOTE_DATABASE));

    // Ensure that the database is added to mapped-databases
    proxy.createDatabase(new Database("newDB", "", new File(localWarehouseUri, "newDB").toURI().toString(), null));
    Federations federations = stopServerAndGetConfiguration();
    PrimaryMetaStore primaryMetaStore = federations.getPrimaryMetaStore();
    assertThat(primaryMetaStore.getMappedDatabases().contains("newdb"), is(true));
  }

  @Test
  public void primaryAndFederatedMappedTables() throws Exception {
    String localTable = "other_local_table";
    String remoteTable = "other_remote_table";
    createLocalTable(new File(localWarehouseUri, LOCAL_DATABASE + "/" + localTable), localTable);
    createLocalTable(new File(localWarehouseUri, LOCAL_DATABASE + "/" + "not_mapped_local"), "not_mapped_local");
    createRemoteTable(new File(remoteWarehouseUri, REMOTE_DATABASE + "/" + remoteTable), remoteTable);
    createRemoteTable(new File(remoteWarehouseUri, REMOTE_DATABASE + "/" + "not_mapped_remote"), "not_mapped_remote");

    MappedTables mappedTablesLocal = new MappedTables(LOCAL_DATABASE, Collections.singletonList(localTable));
    MappedTables mappedTablesRemote = new MappedTables(REMOTE_DATABASE, Collections.singletonList(remoteTable));

    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .withPrimaryMappedTables(Collections.singletonList(mappedTablesLocal))
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(),
            Collections.singletonList(mappedTablesRemote), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();
    List<String> resultTables = proxy.getAllTables(LOCAL_DATABASE);
    assertThat(resultTables.size(), is(1));
    assertThat(resultTables.get(0), is(localTable));

    resultTables = proxy.getAllTables(PREFIXED_REMOTE_DATABASE);
    assertThat(resultTables.size(), is(1));
    assertThat(resultTables.get(0), is(remoteTable));
  }

  @Test
  public void getTablesFromPatternMappedTables() throws Exception {
    String localTable = "other_local_table";
    String remoteTable = "other_remote_table";
    createLocalTable(new File(localWarehouseUri, LOCAL_DATABASE + "/" + localTable), localTable);
    createRemoteTable(new File(remoteWarehouseUri, REMOTE_DATABASE + "/" + remoteTable), remoteTable);

    MappedTables mappedTablesLocal = new MappedTables(LOCAL_DATABASE, Collections.singletonList(".*other.*"));
    MappedTables mappedTablesRemote = new MappedTables(REMOTE_DATABASE, Collections.singletonList("no_match.*"));

    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .withPrimaryMappedTables(Collections.singletonList(mappedTablesLocal))
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(),
            Collections.singletonList(mappedTablesRemote), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();
    List<String> resultTables = proxy.getAllTables(LOCAL_DATABASE);
    assertThat(resultTables.size(), is(1));
    assertThat(resultTables.get(0), is(localTable));

    resultTables = proxy.getAllTables(PREFIXED_REMOTE_DATABASE);
    assertThat(resultTables.size(), is(0));
  }

  @Test
  public void prefixedModeDatabaseNameMapping() throws Exception {
    Map<String, String> databaseNameMapping1 = new HashMap<>();
    databaseNameMapping1.put(LOCAL_DATABASE, "abc");
    Map<String, String> databaseNameMapping2 = new HashMap<>();
    databaseNameMapping2.put(REMOTE_DATABASE, "xyz");
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .withPrimaryDatabaseNameMappingMap(databaseNameMapping1)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .withFederatedDatabaseNameMappingMap(databaseNameMapping2)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    List<String> allDatabases = proxy.getAllDatabases();
    assertThat(allDatabases.size(), is(5));
    assertThat(allDatabases.get(0), is("default"));
    assertThat(allDatabases.get(1), is(LOCAL_DATABASE));
    assertThat(allDatabases.get(2), is("abc"));
    assertThat(allDatabases.get(3), is(PREFIXED_REMOTE_DATABASE));
    assertThat(allDatabases.get(4), is(SECONDARY_METASTORE_NAME + "_xyz"));
    // Local table
    Table waggledLocalTable = proxy.getTable("abc", LOCAL_TABLE);
    assertNotNull(waggledLocalTable);

    // Remote table
    Table waggledRemoteTable = proxy.getTable(SECONDARY_METASTORE_NAME + "_xyz", REMOTE_TABLE);
    assertNotNull(waggledRemoteTable);
  }

  @Test
  public void manualModeDatabaseNameMapping() throws Exception {
    Map<String, String> databaseNameMapping1 = new HashMap<>();
    databaseNameMapping1.put(LOCAL_DATABASE, "abc");
    Map<String, String> databaseNameMapping2 = new HashMap<>();
    databaseNameMapping2.put(REMOTE_DATABASE, "xyz");
    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .withPrimaryDatabaseNameMappingMap(databaseNameMapping1)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .withFederatedDatabaseNameMappingMap(databaseNameMapping2)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    List<String> allDatabases = proxy.getAllDatabases();
    assertThat(allDatabases.size(), is(5));
    assertThat(allDatabases.get(0), is("default"));
    assertThat(allDatabases.get(1), is(LOCAL_DATABASE));
    assertThat(allDatabases.get(2), is("abc"));
    assertThat(allDatabases.get(3), is(REMOTE_DATABASE));
    assertThat(allDatabases.get(4), is("xyz"));
    // Local table
    Table waggledLocalTable = proxy.getTable("abc", LOCAL_TABLE);
    assertNotNull(waggledLocalTable);

    // Remote table
    Table waggledRemoteTable = proxy.getTable("xyz", REMOTE_TABLE);
    assertNotNull(waggledRemoteTable);
  }

  @Test
  public void hiveMetastoreFilterHookConfiguredForPrimary() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .withHiveMetastoreFilterHook(PrefixingMetastoreFilter.class.getName())
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    Table waggledLocalTable = proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
    assertThat(waggledLocalTable.getSd().getLocation(), startsWith("prefix"));

    Table remoteTable = proxy.getTable(REMOTE_DATABASE, REMOTE_TABLE);
    assertThat(remoteTable.getSd().getLocation().startsWith("prefix"), is(false));
  }

  @Test
  public void get_privilege_set() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    HiveObjectType objectType = HiveObjectType.DATABASE;
    String dbName = LOCAL_DATABASE;
    // Explicitly set to null as this threw errors
    String objectName = null;
    List<String> partValues = null;
    String columnName = null;
    HiveObjectRef hiveObjectRef = new HiveObjectRef(objectType, dbName, objectName, partValues, columnName);
    PrincipalPrivilegeSet get_privilege_set = proxy.get_privilege_set(hiveObjectRef, "hadoop", null);
    assertNotNull(get_privilege_set);
  }

  @Test
  public void getTableMeta() throws Exception {
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .federate(SECONDARY_METASTORE_NAME, remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    List<TableMeta> tableMeta = proxy
        .getTableMeta("waggle_remote_remote_database", "*", Lists.newArrayList("EXTERNAL_TABLE"));
    assertThat(tableMeta.size(), is(1));
    assertThat(tableMeta.get(0).getDbName(), is("waggle_remote_remote_database"));
    assertThat(tableMeta.get(0).getTableName(), is(REMOTE_TABLE));
    // use wildcards: '.'
    tableMeta = proxy.getTableMeta("waggle_remote.remote_database", "*", Lists.newArrayList("EXTERNAL_TABLE"));
    assertThat(tableMeta.size(), is(1));
    assertThat(tableMeta.get(0).getDbName(), is("waggle_remote_remote_database"));
    assertThat(tableMeta.get(0).getTableName(), is(REMOTE_TABLE));
    tableMeta = proxy.getTableMeta("waggle.remote_remote_database", "*", Lists.newArrayList("EXTERNAL_TABLE"));
    assertThat(tableMeta.size(), is(1));
    assertThat(tableMeta.get(0).getDbName(), is("waggle_remote_remote_database"));
    assertThat(tableMeta.get(0).getTableName(), is(REMOTE_TABLE));
  }

  @Test
  public void getTableWhenConnectionUnavailablePrefix() throws Exception {
    String unavailable = "unavailable_secondary";
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .federate(unavailable, "thrift://localhost:0000", REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    try {
      // secondary is not reachable
      proxy.getTable(unavailable + "_remote_database", REMOTE_TABLE);
      fail("Should throw TApplicationException");
    } catch (TApplicationException e) {
      // primary still works
      Table tableFromPrimary = proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
      assertNotNull(tableFromPrimary);
    }
  }

  @Test
  public void getTableWhenConnectionUnavailableManual() throws Exception {
    String unavailable = "unavailable_secondary";
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.MANUAL)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .federate(unavailable, "thrift://localhost:0000", REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = runner.createWaggleDanceClient();

    try {
      // secondary is not reachable
      proxy.getTable(REMOTE_DATABASE, REMOTE_TABLE);
      fail("Should throw NoSuchObjectException");
      // Manual mapping works differently then PREFIXED mapping if the secondary is not reachable, it results in NoSuchObject.
      // A better test would be a connection failure after WD started, but that's hard to mimic. So testing current behavior so we at least capture that.
    } catch (NoSuchObjectException e) {
      // primary still works
      Table tableFromPrimary = proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
      assertNotNull(tableFromPrimary);
    }
  }
}
