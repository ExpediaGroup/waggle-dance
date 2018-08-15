package com.hotels.bdp.waggledance.metrics;

import static org.junit.Assert.fail;

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
  public void typical() throws Exception {

    graphiteConfiguration.setHost("localhost");
    graphiteConfiguration.setPort(graphite.port());
    graphiteConfiguration.setPrefix("graphitePrefix");
    graphiteConfiguration.setPollInterval(1000);
    graphiteConfiguration.init();

    monitoringConfiguration = new MonitoringConfiguration();
    monitoringConfiguration.setGraphiteConfiguration(graphiteConfiguration);
    monitoringConfiguration.setMetricRegistry(metricRegistry);

    monitoringConfiguration.init();
    monitoringConfiguration.destroy();

    Set<String> metrics = new TreeSet<>(Arrays.asList(new String(graphite.getOutput()).split("\n")));
    for (String metric : metrics) {
      System.out.println(metric);
    }
    // assertMetric(metrics,
    // "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_all_databases.all.calls.count
    // 2");
    // assertMetric(metrics,
    // "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_all_databases.all.success.count
    // 2");
    // assertMetric(metrics,
    // "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_table_req.primary.calls.count
    // 1");
    // assertMetric(metrics,
    // "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_table_req.primary.success.count
    // 1");
    // assertMetric(metrics,
    // "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_table_req.remote.calls.count
    // 1");
    // assertMetric(metrics,
    // "graphitePrefix.counter.com.hotels.bdp.waggledance.server.FederatedHMSHandler.get_table_req.remote.success.count
    // 1");
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
