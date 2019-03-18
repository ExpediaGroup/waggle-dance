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
package com.hotels.bdp.waggledance.metrics;

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Rule;
import org.junit.Test;

import io.micrometer.graphite.GraphiteMeterRegistry;

import com.hotels.bdp.waggledance.conf.GraphiteConfiguration;
import com.hotels.bdp.waggledance.junit.ServerSocketRule;

public class MonitoringConfigurationIntegrationTest {

  public @Rule ServerSocketRule graphite = new ServerSocketRule();
  private final GraphiteConfiguration graphiteConfiguration = new GraphiteConfiguration();

  @Test
  public void graphiteReporterAllMetricsAreLoggedWhenPollNotCalled() throws Exception {
    String graphitePrefix = "graphitePrefix";

    graphiteConfiguration.setHost("localhost");
    graphiteConfiguration.setPort(graphite.port());
    graphiteConfiguration.setPrefix(graphitePrefix);

    // Using a very long poll interval so it won't actually poll in the test, this is because we want to test that the
    // GraphiteReporter (ScheduledReporter) report() method is called to flush the remaining metrics before closing.
    long longPollInterval = 1000000;
    graphiteConfiguration.setPollInterval(longPollInterval);
    graphiteConfiguration.init();

    MonitoringConfiguration monitoringConfiguration = new MonitoringConfiguration();
    GraphiteMeterRegistry graphiteMeterRegistry = monitoringConfiguration.graphiteMeterRegistry(graphiteConfiguration);

    graphiteMeterRegistry.counter("test-counter").increment();
    graphiteMeterRegistry.close();

    Set<String> metrics = new TreeSet<>(Arrays.asList(new String(graphite.getOutput()).split("\n")));
    assertMetricContainsPrefix(metrics, graphitePrefix + ".test-counter");
  }

  private void assertMetricContainsPrefix(Set<String> metrics, String partialMetric) {
    for (String metric : metrics) {
      if (metric.startsWith(partialMetric)) {
        return;
      }
    }
    fail(String.format("Metric '%s' not found", partialMetric));
  }
}
