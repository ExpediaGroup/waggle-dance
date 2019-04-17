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
package com.hotels.bdp.waggledance;

import static org.apache.directory.api.util.Strings.isNotEmpty;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.springframework.context.ApplicationContext;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.waggledance.api.model.AccessControlType;
import com.hotels.bdp.waggledance.api.model.DatabaseResolution;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.Federations;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;
import com.hotels.bdp.waggledance.conf.GraphiteConfiguration;
import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import com.hotels.bdp.waggledance.conf.YamlStorageConfiguration;
import com.hotels.bdp.waggledance.server.MetaStoreProxyServer;
import com.hotels.bdp.waggledance.yaml.YamlFactory;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;

public class WaggleDanceRunner implements WaggleDance.ContextListener {

  public static final String SERVER_CONFIG = "server-config";
  public static final String FEDERATION_CONFIG = "federation-config";

  private final File serverConfig;
  private final File federationConfig;

  private ApplicationContext applicationContext;

  public static class Builder {
    private final File workingDirectory;
    private final WaggleDanceConfiguration waggleDanceConfiguration = new WaggleDanceConfiguration();
    private final YamlStorageConfiguration yamlStorageConfiguration = new YamlStorageConfiguration();
    private final GraphiteConfiguration graphiteConfiguration = new GraphiteConfiguration();
    private final List<FederatedMetaStore> federatedMetaStores = new ArrayList<>();
    private PrimaryMetaStore primaryMetaStore;

    private Builder(File workingDirectory) {
      checkArgument(workingDirectory != null);
      this.workingDirectory = workingDirectory;
      // Overriding default, makes the tests run a lot quicker
      waggleDanceConfiguration.setThriftServerStopTimeoutValInSeconds(1);
      waggleDanceConfiguration.setThriftServerRequestTimeout(1);
    }

    public Builder verbose(boolean verbose) {
      waggleDanceConfiguration.setVerbose(verbose);
      return this;
    }

    public Builder port(int port) {
      waggleDanceConfiguration.setPort(port);
      return this;
    }

    public Builder databaseResolution(DatabaseResolution databaseResolution) {
      waggleDanceConfiguration.setDatabaseResolution(databaseResolution);
      return this;
    }

    public Builder overwriteConfigOnShutdown(boolean overwriteConfigOnShutdown) {
      yamlStorageConfiguration.setOverwriteConfigOnShutdown(overwriteConfigOnShutdown);
      return this;
    }

    public Builder disconnectionDelay(int disconnectConnectionDelay, TimeUnit disconnectTimeUnit) {
      waggleDanceConfiguration.setDisconnectConnectionDelay(disconnectConnectionDelay);
      waggleDanceConfiguration.setDisconnectTimeUnit(disconnectTimeUnit);
      return this;
    }

    public Builder configurationProperty(String key, String value) {
      if (waggleDanceConfiguration.getConfigurationProperties() == null) {
        waggleDanceConfiguration.setConfigurationProperties(new HashMap<String, String>());
      }
      waggleDanceConfiguration.getConfigurationProperties().put(key, value);
      return this;
    }

    public Builder federate(String name, String remoteMetaStoreUris, String... mappableDatabases) {
      checkArgument(isNotEmpty(name));
      checkArgument(isNotEmpty(remoteMetaStoreUris));
      FederatedMetaStore federatedMetaStore = new FederatedMetaStore(name, remoteMetaStoreUris);
      federatedMetaStore.setMappedDatabases(Arrays.asList(mappableDatabases));
      federatedMetaStore.setLatency(8000L);
      federatedMetaStores.add(federatedMetaStore);
      return this;
    }

    public Builder federate(
        String name,
        String remoteMetaStoreUris,
        AccessControlType accessControlType,
        String[] mappableDatabases,
        String[] writeableDatabaseWhiteList) {
      checkArgument(isNotEmpty(name));
      checkArgument(isNotEmpty(remoteMetaStoreUris));
      FederatedMetaStore federatedMetaStore = new FederatedMetaStore(name, remoteMetaStoreUris, accessControlType);
      federatedMetaStore.setMappedDatabases(Arrays.asList(mappableDatabases));
      federatedMetaStore.setWritableDatabaseWhiteList(Arrays.asList(writeableDatabaseWhiteList));
      federatedMetaStore.setLatency(8000L);
      federatedMetaStores.add(federatedMetaStore);
      return this;
    }

    public Builder federateWithMetastoreTunnel(
        String name,
        String remoteMetaStoreUris,
        String mappableDatabases,
        String route,
        String privateKeys,
        String knownHosts) {
      FederatedMetaStore federatedMetaStore = new FederatedMetaStore(name, remoteMetaStoreUris);
      federatedMetaStore.setMappedDatabases(Arrays.asList(mappableDatabases));

      MetastoreTunnel metastoreTunnel = new MetastoreTunnel();
      metastoreTunnel.setRoute(route);
      metastoreTunnel.setPrivateKeys(privateKeys);
      metastoreTunnel.setKnownHosts(knownHosts);
      federatedMetaStore.setMetastoreTunnel(metastoreTunnel);
      federatedMetaStore.setLatency(8000L);
      federatedMetaStores.add(federatedMetaStore);
      return this;
    }

    public Builder primary(
        String name,
        String remoteMetaStoreUris,
        AccessControlType accessControlType,
        String... writableDatabaseWhiteList) {
      checkArgument(isNotEmpty(name));
      checkArgument(isNotEmpty(remoteMetaStoreUris));
      primaryMetaStore = new PrimaryMetaStore(name, remoteMetaStoreUris, accessControlType, writableDatabaseWhiteList);
      primaryMetaStore.setLatency(8000L);
      return this;
    }

    public Builder primaryWithPrefix(
        String name,
        String remoteMetaStoreUris,
        AccessControlType accessControlType,
        String... writableDatabaseWhiteList) {
      checkArgument(isNotEmpty(name));
      checkArgument(isNotEmpty(remoteMetaStoreUris));
      primaryMetaStore = new PrimaryMetaStore(name, remoteMetaStoreUris, accessControlType, writableDatabaseWhiteList);
      primaryMetaStore.setLatency(8000L);
      primaryMetaStore.setDatabasePrefix(name + "_");
      return this;
    }

    public Builder graphite(String graphiteHost, int graphitePort, String graphitePrefix, long pollInterval) {
      graphiteConfiguration.setHost(graphiteHost);
      graphiteConfiguration.setPort(graphitePort);
      graphiteConfiguration.setPrefix(graphitePrefix);
      graphiteConfiguration.setPollInterval(pollInterval);
      return this;
    }

    private File marshall(Yaml yaml, String fileName, Object... objects) {
      File config = new File(workingDirectory, fileName);

      FileSystemManager fsManager = null;
      try {
        fsManager = VFS.getManager();
      } catch (FileSystemException e) {
        throw new RuntimeException("Unable to initialize Virtual File System", e);
      }

      try (FileObject target = fsManager.resolveFile(config.toURI());
          Writer writer = new OutputStreamWriter(target.getContent().getOutputStream(), StandardCharsets.UTF_8)) {
        for (Object object : objects) {
          yaml.dump(object, writer);
        }
      } catch (IOException e) {
        throw new RuntimeException("Unable to write federations to '" + config.toURI() + "'", e);
      }

      return config;
    }

    public WaggleDanceRunner build() {
      Yaml yaml = YamlFactory.newYaml();

      HashMap<String, Object> extraConfig = new HashMap<>();
      extraConfig.put("graphite", graphiteConfiguration);
      extraConfig.put("yaml-storage", yamlStorageConfiguration);
      File serverConfig = marshall(yaml, SERVER_CONFIG + ".yml", waggleDanceConfiguration, extraConfig);

      Federations federations = new Federations(primaryMetaStore, federatedMetaStores);
      File federationConfig = marshall(yaml, FEDERATION_CONFIG + ".yml", federations);

      WaggleDanceRunner wgRunner = new WaggleDanceRunner(serverConfig, federationConfig);

      return wgRunner;
    }

  }

  public static Builder builder(File workingDirectory) {
    return new Builder(workingDirectory);
  }

  private WaggleDanceRunner(File serverConfig, File federationConfig) {
    this.serverConfig = serverConfig;
    this.federationConfig = federationConfig;
  }

  public File serverConfig() {
    return serverConfig;
  }

  public File federationConfig() {
    return federationConfig;
  }

  private Map<String, String> populateProperties() {
    ImmutableMap.Builder<String, String> builder = ImmutableMap
        .<String, String>builder()
        // Logging
        .put("logging.config", "classpath:log4j2.xml")
        // Configuration files
        .put(SERVER_CONFIG, serverConfig.getAbsolutePath())
        .put(FEDERATION_CONFIG, federationConfig.getAbsolutePath());
    return builder.build();
  }

  private static String[] getArgsArray(Map<String, String> props) {
    String[] args = FluentIterable.from(props.entrySet()).transform(new Function<Entry<String, String>, String>() {
      @Override
      public String apply(Entry<String, String> input) {
        return "--" + input.getKey() + "=" + input.getValue();
      }
    }).toArray(String.class);
    return args;
  }

  private MetaStoreProxyServer getProxy() {
    return applicationContext.getBean(MetaStoreProxyServer.class);
  }

  public Map<String, String> run() throws Exception {
    Map<String, String> props = populateProperties();
    WaggleDance.register(this);
    WaggleDance.main(getArgsArray(props));
    return props;
  }

  public void waitForService() throws Exception {
    long delay = 1;
    while (applicationContext == null) {
      if (delay >= 15) {
        throw new TimeoutException("Service did not start");
      }
      Thread.sleep(TimeUnit.SECONDS.toMillis(++delay));
    }
    getProxy().waitUntilStarted();
  }

  public void stop() throws Exception {
    if (applicationContext != null) {
      getProxy().stop();
      long delay = 1;
      while (applicationContext != null) {
        if (delay >= 15) {
          throw new TimeoutException("Service did not stop");
        }
        Thread.sleep(TimeUnit.SECONDS.toMillis(++delay));
      }
    }
  }

  @Override
  public void onStart(ApplicationContext context) {
    applicationContext = context;
  }

  @Override
  public void onStop(ApplicationContext context) {
    applicationContext = null;
  }

}
