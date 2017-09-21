/**
 * Copyright (C) 2016-2017 Expedia Inc.
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static com.hotels.bdp.waggledance.TestUtils.createPartitionedTable;
import static com.hotels.bdp.waggledance.TestUtils.createUnpartitionedTable;
import static com.hotels.bdp.waggledance.TestUtils.newPartition;
import static com.hotels.bdp.waggledance.api.model.AccessControlType.READ_AND_WRITE_ON_DATABASE_WHITELIST;
import static com.hotels.bdp.waggledance.api.model.AccessControlType.READ_ONLY;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import feign.Feign;
import feign.jackson.JacksonDecoder;
import feign.jackson.JacksonEncoder;
import feign.jaxrs.JAXRSContract;
import fm.last.commons.test.file.ClassDataFolder;
import fm.last.commons.test.file.DataFolder;

import com.hotels.bdp.waggledance.api.model.AccessControlType;
import com.hotels.bdp.waggledance.api.model.DatabaseResolution;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.Federations;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;
import com.hotels.bdp.waggledance.junit.ServerSocketRule;
import com.hotels.bdp.waggledance.server.MetaStoreProxyServer;
import com.hotels.bdp.waggledance.yaml.YamlFactory;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

public class WaggleDanceIntegrationTest {
  private static final Logger LOG = LoggerFactory.getLogger(WaggleDanceIntegrationTest.class);

  private static final String LOCAL_DATABASE = "local_database";
  private static final String LOCAL_TABLE = "local_table";
  private static final String REMOTE_DATABASE = "remote_database";
  private static final String REMOTE_TABLE = "remote_table";

  public @Rule ServerSocketRule graphite = new ServerSocketRule();
  public @Rule ExpectedSystemExit exit = ExpectedSystemExit.none();
  public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
  public @Rule ThriftHiveMetaStoreJUnitRule localServer = new ThriftHiveMetaStoreJUnitRule(LOCAL_DATABASE);
  public @Rule ThriftHiveMetaStoreJUnitRule remoteServer = new ThriftHiveMetaStoreJUnitRule(REMOTE_DATABASE);
  public @Rule ThriftHiveMetaStoreJUnitRule newRemoteServer = new ThriftHiveMetaStoreJUnitRule();
  public @Rule DataFolder dataFolder = new ClassDataFolder();

  private ExecutorService executor;
  private WaggleDanceRunner runner;

  private File configLocation;
  private File localWarehouseUri;
  private File remoteWarehouseUri;

  @Before
  public void init() throws Exception {
    configLocation = temporaryFolder.newFolder("config");
    localWarehouseUri = temporaryFolder.newFolder("local-warehouse");
    remoteWarehouseUri = temporaryFolder.newFolder("remote-warehouse");

    createLocalTable(new File(localWarehouseUri, LOCAL_DATABASE + "/" + LOCAL_TABLE));
    LOG.info(">>>> Table {} ", localServer.client().getTable(LOCAL_DATABASE, LOCAL_TABLE));

    createRemoteTable(new File(remoteWarehouseUri, REMOTE_DATABASE + "/" + REMOTE_TABLE));
    LOG.info(">>>> Table {} ", remoteServer.client().getTable(REMOTE_DATABASE, REMOTE_TABLE));

    executor = Executors.newSingleThreadExecutor();
  }

  @After
  public void destroy() throws Exception {
    if (runner != null) {
      runner.stop();
    }
    executor.shutdownNow();
  }

  private void createLocalTable(File tableUri) throws Exception {
    createUnpartitionedTable(localServer.client(), LOCAL_DATABASE, LOCAL_TABLE, tableUri);
  }

  private void createRemoteTable(File tableUri) throws Exception {
    HiveMetaStoreClient client = remoteServer.client();

    Table hiveTable = createPartitionedTable(client, REMOTE_DATABASE, REMOTE_TABLE, tableUri);

    File partitionEurope = new File(tableUri, "continent=Europe");
    File partitionUk = new File(partitionEurope, "country=UK");

    File partitionAsia = new File(tableUri, "continent=Asia");
    File partitionChina = new File(partitionAsia, "country=China");

    LOG.info(">>>> Partitions added: {}",
        client.add_partitions(Arrays.asList(newPartition(hiveTable, Arrays.asList("Europe", "UK"), partitionUk),
            newPartition(hiveTable, Arrays.asList("Asia", "China"), partitionChina))));
  }

  private String getWaggleDanceThriftUri() {
    return "thrift://localhost:" + MetaStoreProxyServer.DEFAULT_WAGGLEDANCE_PORT;
  }

  private HiveMetaStoreClient getWaggleDanceClient() throws MetaException {
    HiveConf conf = new HiveConf();
    conf.setVar(ConfVars.METASTOREURIS, getWaggleDanceThriftUri());
    conf.setBoolVar(ConfVars.METASTORE_EXECUTE_SET_UGI, true);
    return new HiveMetaStoreClient(conf);
  }

  private void runWaggleDance(final WaggleDanceRunner runner) throws Exception {
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          runner.run();
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException("Error during execution", e);
        }
      }
    });
    runner.waitForService();
  }

  private Federations stopServerAndGetConfiguration() throws Exception, FileNotFoundException {
    runner.stop();
    // Stopping the server triggers the saving of the config file.
    Federations federations = YamlFactory.newYaml().loadAs(new FileInputStream(runner.federationConfig()),
        Federations.class);
    return federations;
  }

  @Test
  public void typical() throws Exception {
    exit.expectSystemExitWithStatus(0);
    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .federate("waggle_remote", remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = getWaggleDanceClient();

    // Local table
    Table localTable = localServer.client().getTable(LOCAL_DATABASE, LOCAL_TABLE);
    Table waggledLocalTable = proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
    assertThat(waggledLocalTable, is(localTable));

    // Remote table
    String waggledRemoteDbName = REMOTE_DATABASE;
    assertTypicalRemoteTable(proxy, waggledRemoteDbName);
  }

  @Test
  public void usePrefix() throws Exception {
    exit.expectSystemExitWithStatus(0);
    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .federate("waggle_remote", remoteServer.getThriftConnectionUri())
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = getWaggleDanceClient();

    // Local table
    Table localTable = localServer.client().getTable(LOCAL_DATABASE, LOCAL_TABLE);
    Table waggledLocalTable = proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
    assertThat(waggledLocalTable, is(localTable));

    // Remote table
    String waggledRemoteDbName = "waggle_remote_" + REMOTE_DATABASE;
    assertTypicalRemoteTable(proxy, waggledRemoteDbName);

  }

  private void assertTypicalRemoteTable(HiveMetaStoreClient proxy, String waggledRemoteDbName)
    throws MetaException, TException, NoSuchObjectException {
    Table remoteTable = remoteServer.client().getTable(REMOTE_DATABASE, REMOTE_TABLE);
    Table waggledRemoteTable = proxy.getTable(waggledRemoteDbName, REMOTE_TABLE);
    assertThat(waggledRemoteTable.getDbName(), is(waggledRemoteDbName));
    assertThat(waggledRemoteTable.getTableName(), is(remoteTable.getTableName()));
    assertThat(waggledRemoteTable.getSd(), is(remoteTable.getSd()));
    assertThat(waggledRemoteTable.getParameters(), is(remoteTable.getParameters()));
    assertThat(waggledRemoteTable.getPartitionKeys(), is(remoteTable.getPartitionKeys()));

    List<String> partitionNames = Arrays.asList("continent=Europe/country=UK", "continent=Asia/country=China");

    List<Partition> remotePartitions = remoteServer.client().getPartitionsByNames(REMOTE_DATABASE, REMOTE_TABLE,
        partitionNames);
    List<Partition> waggledRemotePartitions = proxy.getPartitionsByNames(waggledRemoteDbName, REMOTE_TABLE,
        partitionNames);
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

  @Test
  public void typicalWithGraphite() throws Exception {
    exit.expectSystemExitWithStatus(0);

    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(), READ_ONLY)
        .federate("remote", remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .graphite("localhost", graphite.port(), "graphitePrefix", 1000)
        .build();

    runWaggleDance(runner);
    HiveMetaStoreClient proxy = getWaggleDanceClient();

    // Execute a couple of requests
    proxy.getAllDatabases();
    proxy.getTable(LOCAL_DATABASE, LOCAL_TABLE);
    proxy.getAllDatabases();
    proxy.getTable(REMOTE_DATABASE, REMOTE_TABLE);
    runner.stop();

    Set<String> metrics = new TreeSet<>(Arrays.asList(new String(graphite.getOutput()).split("\n")));
    // print(metrics);
    assertMetric(metrics,
        "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_all_databases.all.calls.count 2");
    assertMetric(metrics,
        "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_all_databases.all.success.count 2");
    assertMetric(metrics,
        "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_table_req.primary.calls.count 1");
    assertMetric(metrics,
        "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_table_req.primary.success.count 1");
    assertMetric(metrics,
        "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_table_req.remote.calls.count 1");
    assertMetric(metrics,
        "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_table_req.remote.success.count 1");
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
    exit.expectSystemExitWithStatus(0);
    String writableDatabase = "writable_db";

    localServer.createDatabase(writableDatabase);

    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(), AccessControlType.READ_AND_WRITE_AND_CREATE)
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = getWaggleDanceClient();
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
  public void databaseWhitelisting() throws Exception {
    exit.expectSystemExitWithStatus(0);
    String writableDatabase = "writable_db";

    localServer.createDatabase(writableDatabase);

    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(), READ_AND_WRITE_ON_DATABASE_WHITELIST,
            writableDatabase)
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = getWaggleDanceClient();
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
    exit.expectSystemExitWithStatus(0);

    runner = WaggleDanceRunner
        .builder(configLocation)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = getWaggleDanceClient();
    proxy.createDatabase(new Database("newDB", "", new File(localWarehouseUri, "newDB").toURI().toString(), null));
    Database newDB = proxy.getDatabase("newDB");
    assertNotNull(newDB);

    // Should be allowed due to newly loaded write privileges
    proxy.alterDatabase("newDB", new Database(newDB));

    Federations federations = stopServerAndGetConfiguration();
    PrimaryMetaStore metaStore = federations.getPrimaryMetaStore();
    assertThat(metaStore.getWritableDatabaseWhiteList().size(), is(1));
    assertThat(metaStore.getWritableDatabaseWhiteList().get(0), is("newdb"));
  }

  @Test
  public void createDatabaseDatabaseUsingPrefixAndWhitelistingUpdates() throws Exception {
    exit.expectSystemExitWithStatus(0);

    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = getWaggleDanceClient();
    proxy.createDatabase(new Database("newDB", "", new File(localWarehouseUri, "newDB").toURI().toString(), null));
    Database newDB = proxy.getDatabase("newDB");
    assertNotNull(newDB);

    // Should be allowed due to newly loaded write privileges
    proxy.alterDatabase("newDB", new Database(newDB));

    Federations federations = stopServerAndGetConfiguration();
    PrimaryMetaStore metaStore = federations.getPrimaryMetaStore();
    assertThat(metaStore.getWritableDatabaseWhiteList().size(), is(1));
    assertThat(metaStore.getWritableDatabaseWhiteList().get(0), is("newdb"));
  }

  @Test
  public void doesNotOverwriteConfigOnShutdownManualMode() throws Exception {
    // Note a similar test for PREFIX is not required
    exit.expectSystemExitWithStatus(0);

    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.MANUAL)
        .overwriteConfigOnShutdown(false)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .federate("waggle_remote", remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);

    HiveMetaStoreClient proxy = getWaggleDanceClient();
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
    exit.expectSystemExitWithStatus(0);

    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .federate("waggle_remote", remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    FederationsAdminClient restClient = Feign
        .builder()
        .contract(new JAXRSContract())
        .encoder(new JacksonEncoder())
        .decoder(new JacksonDecoder())
        .target(FederationsAdminClient.class, "http://localhost:18000/");

    FederatedMetaStore newFederation = new FederatedMetaStore("new_waggle_remote",
        newRemoteServer.getThriftConnectionUri());
    restClient.add(newFederation);

    Federations federations = stopServerAndGetConfiguration();

    List<FederatedMetaStore> federatedMetastores = federations.getFederatedMetaStores();
    assertThat(federatedMetastores.size(), is(2));

    FederatedMetaStore remoteMetastore = federatedMetastores.get(0);
    assertThat(remoteMetastore.getName(), is("waggle_remote"));
    assertThat(remoteMetastore.getMappedDatabases().size(), is(1));
    assertThat(remoteMetastore.getMappedDatabases().get(0), is(REMOTE_DATABASE));

    FederatedMetaStore newRemoteMetastore = federatedMetastores.get(1);
    assertThat(newRemoteMetastore.getName(), is("new_waggle_remote"));
    assertThat(newRemoteMetastore.getMappedDatabases().size(), is(0));
  }

  @Test
  public void doesNotOverwriteConfigOnShutdownAfterAddingFederation() throws Exception {
    exit.expectSystemExitWithStatus(0);

    runner = WaggleDanceRunner
        .builder(configLocation)
        .databaseResolution(DatabaseResolution.PREFIXED)
        .overwriteConfigOnShutdown(false)
        .primary("primary", localServer.getThriftConnectionUri(),
            AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST)
        .federate("waggle_remote", remoteServer.getThriftConnectionUri(), REMOTE_DATABASE)
        .build();

    runWaggleDance(runner);
    FederationsAdminClient restClient = Feign
        .builder()
        .contract(new JAXRSContract())
        .encoder(new JacksonEncoder())
        .decoder(new JacksonDecoder())
        .target(FederationsAdminClient.class, "http://localhost:18000/");

    FederatedMetaStore newFederation = new FederatedMetaStore("new_waggle_remote",
        newRemoteServer.getThriftConnectionUri());
    restClient.add(newFederation);

    Federations federations = stopServerAndGetConfiguration();

    List<FederatedMetaStore> federatedMetastores = federations.getFederatedMetaStores();
    assertThat(federatedMetastores.size(), is(1));

    FederatedMetaStore remoteMetastore = federatedMetastores.get(0);
    assertThat(remoteMetastore.getName(), is("waggle_remote"));
    assertThat(remoteMetastore.getMappedDatabases().size(), is(1));
    assertThat(remoteMetastore.getMappedDatabases().get(0), is(REMOTE_DATABASE));
  }

}
