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

import static org.junit.Assert.fail;
import static org.mockito.Matchers.contains;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Rule;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

import com.hotels.bdp.waggledance.conf.GraphiteConfiguration;
import com.hotels.bdp.waggledance.junit.ServerSocketRule;

public class MonitoringConfigurationIntegrationTest {

  public @Rule ServerSocketRule graphite = new ServerSocketRule();
  private final GraphiteConfiguration graphiteConfiguration = new GraphiteConfiguration();
  private final MetricRegistry metricRegistry = new MetricRegistry();
  private MonitoringConfiguration monitoringConfiguration;

  @Test
  public void allMetricsAreLoggedWhenPollItervalIsHigh() throws Exception {
    String graphitePrefix = "graphitePrefix";

    graphiteConfiguration.setHost("localhost");
    graphiteConfiguration.setPort(graphite.port());
    graphiteConfiguration.setPrefix(graphitePrefix);
    graphiteConfiguration.setPollInterval(1000);
    graphiteConfiguration.init();

    monitoringConfiguration = new MonitoringConfiguration();
    monitoringConfiguration.setGraphiteConfiguration(graphiteConfiguration);
    monitoringConfiguration.setMetricRegistry(metricRegistry);

    monitoringConfiguration.init();
    monitoringConfiguration.destroy();

    Set<String> metrics = new TreeSet<>(Arrays.asList(new String(graphite.getOutput()).split("\n")));
    assertMetric(metrics, contains(graphitePrefix + ".gc"));
    assertMetric(metrics, contains(graphitePrefix + ".memory"));
    assertMetric(metrics, contains(graphitePrefix + ".threads"));
  }

  private void assertMetric(Set<String> metrics, String partialMetric) {
    for (String metric : metrics) {
      if (metric.startsWith(partialMetric)) {
        return;
      }
    }
    fail(String.format("Metric '%s' not found", partialMetric));
  }
}
