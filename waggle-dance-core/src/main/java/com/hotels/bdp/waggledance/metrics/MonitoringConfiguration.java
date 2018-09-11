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
package com.hotels.bdp.waggledance.metrics;

import java.time.Duration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.config.NamingConvention;
import io.micrometer.core.instrument.util.HierarchicalNameMapper;
import io.micrometer.graphite.GraphiteConfig;
import io.micrometer.graphite.GraphiteMeterRegistry;
import io.micrometer.graphite.GraphiteProtocol;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;

import com.hotels.bdp.waggledance.conf.GraphiteConfiguration;

@Configuration
public class MonitoringConfiguration {

  private final GraphiteConfig DISABLED_GRAPHITE_CONFIG = new GraphiteConfig() {
    @Override
    public boolean enabled() {
      return false;
    }

    @Override
    public String get(String key) {
      return null;
    }
  };

  @Bean
  public GraphiteMeterRegistry graphiteMeterRegistry(GraphiteConfiguration graphiteConfiguration) {
    GraphiteConfig graphiteConfig = DISABLED_GRAPHITE_CONFIG;
    if (graphiteConfiguration.isEnabled()) {

      graphiteConfig = new GraphiteConfig() {

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
        public Duration step() {
          return Duration
              .ofMillis(
                  graphiteConfiguration.getPollIntervalTimeUnit().toMillis(graphiteConfiguration.getPollInterval()));
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
    }
    HierarchicalNameMapper wdHierarchicalNameMapper = (id, convention) -> graphiteConfiguration.getPrefix()
        + "."
        + HierarchicalNameMapper.DEFAULT.toHierarchicalName(id, convention);
    GraphiteMeterRegistry graphiteMeterRegistry = new GraphiteMeterRegistry(graphiteConfig, Clock.SYSTEM,
        wdHierarchicalNameMapper);
    graphiteMeterRegistry.config().namingConvention(NamingConvention.dot);
    return graphiteMeterRegistry;
  }

  @Bean
  public JmxMeterRegistry jmxMeterRegistry() {
    return new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
  }

}
