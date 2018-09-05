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
package com.hotels.bdp.waggledance.context;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.codahale.metrics.graphite.Graphite;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.annotation.Bean;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.graphite.GraphiteConfig;
import io.micrometer.graphite.GraphiteMeterRegistry;
import io.micrometer.graphite.GraphiteProtocol;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIfaceClientFactory;
import com.hotels.bdp.waggledance.client.TunnelingMetaStoreClientFactory;
import com.hotels.bdp.waggledance.conf.GraphiteConfiguration;
import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import com.hotels.bdp.waggledance.mapping.model.ASTQueryMapping;
import com.hotels.bdp.waggledance.mapping.model.QueryMapping;
import com.hotels.bdp.waggledance.mapping.service.PrefixNamingStrategy;
import com.hotels.bdp.waggledance.mapping.service.impl.LowerCasePrefixNamingStrategy;
import org.springframework.context.annotation.Conditional;

@org.springframework.context.annotation.Configuration
public class CommonBeans {

  @Bean
  public MeterRegistry graphiteMeterRegistry(GraphiteConfiguration graphiteConfiguration) {

    GraphiteConfig graphiteConfig = new GraphiteConfig() {

      @Override
      public String host() {
        return graphiteConfiguration.getHost();
      }

      @Override
      public int port() {
        return graphiteConfiguration.getPort();
      }

      @Override
      public boolean enabled() {
        return graphiteConfiguration.isEnabled();
      }

      @Override
      public String[] tagsAsPrefix() {
        return new String[] { graphiteConfiguration.getPrefix() };
      }

      @Override
      public TimeUnit durationUnits() {
        return graphiteConfiguration.getPollIntervalTimeUnit();
      }

      @Override
      public GraphiteProtocol protocol() {
        return GraphiteProtocol.PLAINTEXT;
      }

      @Override
      public String get(String arg0) {
        return null;
      }
    };

    GraphiteMeterRegistry graphiteMeterRegistry = null;
      if (graphiteConfiguration.isEnabled()) {
        HierarchicalNameMapper wdHierarchicalNameMapper = (id, convention) -> graphiteConfiguration.getPrefix() + "." + HierarchicalNameMapper.DEFAULT.toHierarchicalName(id, convention);

        graphiteMeterRegistry = new GraphiteMeterRegistry(graphiteConfig, Clock.SYSTEM,
            wdHierarchicalNameMapper);

        graphiteMeterRegistry.config().namingConvention(NamingConvention.dot);
      }
  else {
        graphiteMeterRegistry = null;
      }
    return graphiteMeterRegistry;
  }

  @Bean
  public HiveConf hiveConf(WaggleDanceConfiguration waggleDanceConfiguration) {
    Map<String, String> confProps = waggleDanceConfiguration.getConfigurationProperties();

    final HiveConf hiveConf = new HiveConf(new Configuration(false), getClass());
    // set all properties specified on the command line
    if (confProps != null) {
      for (Map.Entry<String, String> entry : confProps.entrySet()) {
        hiveConf.set(entry.getKey(), entry.getValue());
      }
    }
    return hiveConf;
  }

  // @Bean
  // public MetricRegistry metricRegistry(WaggleDanceConfiguration waggleDanceConfiguration) {
  // return new MetricRegistry();
  // }

  @Bean
  public PrefixNamingStrategy prefixNamingStrategy(WaggleDanceConfiguration waggleDanceConfiguration) {
    return new LowerCasePrefixNamingStrategy();
  }

  @Bean
  public CloseableThriftHiveMetastoreIfaceClientFactory metaStoreClientFactory() {
    return new CloseableThriftHiveMetastoreIfaceClientFactory(new TunnelingMetaStoreClientFactory());
  }

  @Bean
  public QueryMapping queryMapping() {
    return ASTQueryMapping.INSTANCE;
  }

}
