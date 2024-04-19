/**
 * Copyright (C) 2016-2024 Expedia, Inc.
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
package com.hotels.bdp.waggledance.extensions;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import redis.embedded.RedisServer;

import com.hotels.bdp.waggledance.TestUtils;
import com.hotels.bdp.waggledance.WaggleDanceRunner;
import com.hotels.bdp.waggledance.api.model.AccessControlType;
import com.hotels.bdp.waggledance.api.model.DatabaseResolution;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

public class WaggleDanceRateLimitIntegrationTest {

  public @Rule TemporaryFolder temporaryFolder = new TemporaryFolder();
  public @Rule ThriftHiveMetaStoreJUnitRule metastore = new ThriftHiveMetaStoreJUnitRule();

  private RedisServer redisServer;
  private WaggleDanceRunner runner;
  private Map<String, Object> extraServerConfig;
  
  @Before
  public void setup() {
    extraServerConfig = new HashMap<>();
    extraServerConfig.put("waggledance.extensions.ratelimit.enabled", "true");    
    //Use INTERVALLY as it's more deterministic for the test.
    extraServerConfig.put("waggledance.extensions.ratelimit.refillType", "INTERVALLY"); 
  }
  

  @Test
  public void rateLimitInMemory() throws Exception {
    extraServerConfig.put("waggledance.extensions.ratelimit.storage", "MEMORY");
    extraServerConfig.put("waggledance.extensions.ratelimit.capacity", "2");
    extraServerConfig.put("waggledance.extensions.ratelimit.tokenPerMinute", "1");

    runner = WaggleDanceRunner
        .builder(temporaryFolder.newFolder("config"))
        .databaseResolution(DatabaseResolution.PREFIXED)
        .extraServerConfig(extraServerConfig)
        .primary("primary", metastore.getThriftConnectionUri(), AccessControlType.READ_AND_WRITE_AND_CREATE)
        .build();

    runner.runAndWaitForStartup();

    HiveMetaStoreClient client = runner.createWaggleDanceClient();
    assertTokensUsed(client);
  }

  @Test
  public void rateLimitRedis() throws Exception {
    startRedis();
    String reddisonYaml = "";
    reddisonYaml += "singleServerConfig:\n";
    reddisonYaml += "    address: \"redis://localhost:" + redisServer.ports().get(0) + "\"\n";

    extraServerConfig.put("waggledance.extensions.ratelimit.storage", "REDIS");
    extraServerConfig.put("waggledance.extensions.ratelimit.capacity", "2");
    extraServerConfig.put("waggledance.extensions.ratelimit.tokenPerMinute", "1");
    extraServerConfig.put("waggledance.extensions.ratelimit.reddison.embedded.config", reddisonYaml);

    runner = WaggleDanceRunner
        .builder(temporaryFolder.newFolder("config"))
        .databaseResolution(DatabaseResolution.PREFIXED)
        .extraServerConfig(extraServerConfig)
        .primary("primary", metastore.getThriftConnectionUri(), AccessControlType.READ_AND_WRITE_AND_CREATE)
        .build();

    runner.runAndWaitForStartup();

    HiveMetaStoreClient client = runner.createWaggleDanceClient();
    assertTokensUsed(client);
  }

  private void assertTokensUsed(HiveMetaStoreClient client) throws MetaException, NoSuchObjectException, TException {
    List<String> allDatabases = client.getAllDatabases();
    assertThat(allDatabases.size(), is(2));
    Database database = client.getDatabase("default");
    assertThat(database.getName(), is("default"));

    // Tokens spent
    allDatabases = client.getAllDatabases();
    // getAllDatabases is special as it's a request that is federated across all Metastores
    // hence the underlying Rate Limit Exception is not returned.
    assertThat(allDatabases.size(), is(0));

    try {
      client.getDatabase("default");
    } catch (MetaException e) {
      assertThat(e.getMessage(), is("Waggle Dance: [STATUS=429] Too many requests."));
    }
  }

  @Test
  public void ignoreRedisServerDown() throws Exception {
    startRedis();
    String reddisonYaml = "";
    reddisonYaml += "singleServerConfig:\n";
    reddisonYaml += "    address: \"redis://localhost:" + redisServer.ports().get(0) + "\"\n";
    reddisonYaml += "    retryAttempts: 0\n";

    extraServerConfig.put("waggledance.extensions.ratelimit.storage", "REDIS");
    extraServerConfig.put("waggledance.extensions.ratelimit.capacity", "1");
    extraServerConfig.put("waggledance.extensions.ratelimit.tokenPerMinute", "1");
    extraServerConfig.put("waggledance.extensions.ratelimit.reddison.embedded.config", reddisonYaml);

    runner = WaggleDanceRunner
        .builder(temporaryFolder.newFolder("config"))
        .databaseResolution(DatabaseResolution.PREFIXED)
        .extraServerConfig(extraServerConfig)
        .primary("primary", metastore.getThriftConnectionUri(), AccessControlType.READ_AND_WRITE_AND_CREATE)
        .build();

    runner.runAndWaitForStartup();

    // Simulate Redis server down
    redisServer.stop();

    HiveMetaStoreClient client = runner.createWaggleDanceClient();
    assertThat(client.getDatabase("default").getName(), is("default"));
    assertThat(client.getDatabase("default").getName(), is("default"));
    assertThat(client.getDatabase("default").getName(), is("default"));
  }

  @After
  public void teardown() throws Exception {
    if (redisServer != null) {
      redisServer.stop();
    }
    if (runner != null) {
      runner.stop();
    }
  }

  private void startRedis() throws Exception {
    redisServer = new RedisServer(TestUtils.getFreePort());
    redisServer.start();
  }
}
