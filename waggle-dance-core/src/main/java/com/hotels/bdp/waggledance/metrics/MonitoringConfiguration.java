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

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.graphite.GraphiteMeterRegistry;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;

import com.hotels.bdp.waggledance.conf.GraphiteConfiguration;

@Configuration
public class MonitoringConfiguration {
  private static final Logger LOG = LoggerFactory.getLogger(MonitoringConfiguration.class);

  private final Set<Closeable> reporters = new HashSet<>();
  //private @Autowired MetricRegistry metricRegistry;
  private @Autowired GraphiteConfiguration graphiteConfiguration;
  private @Autowired GraphiteMeterRegistry graphiteMeterRegistry;

  private <R extends Closeable> R registerReporter(R reporter) {
    reporters.add(reporter);
    return reporter;
  }

  @PreDestroy
  public void destroy() {
    if (reporters != null) {
      for (Closeable reporter : reporters) {
        try {
          if (reporter instanceof ScheduledReporter) {
            ((ScheduledReporter) reporter).report();
          }
          reporter.close();
        } catch (Exception e) {
          LOG.warn("Problem closing reporter", e);
        }
      }
    }
  }

  @PostConstruct()
  public void init() {
    registerBaseMetrics();
    // registerReporter(jmxReporterBuilder().build()).start();
    if (graphiteConfiguration.isEnabled()) {
      // GraphiteReporter graphiteReporter = graphiteReporterBuilder().build(newGraphite());
      // registerReporter(graphiteReporter);
      // graphiteMeterRegistry.start();
    }
  }

  private void registerBaseMetrics() {
    new JvmThreadMetrics().bindTo(graphiteMeterRegistry);
    new JvmGcMetrics().bindTo(graphiteMeterRegistry);
    new JvmMemoryMetrics().bindTo(graphiteMeterRegistry);
    new JvmThreadMetrics().bindTo(graphiteMeterRegistry);

    //
    // metricRegistry.register("gc", new GarbageCollectorMetricSet());
    // metricRegistry.register("memory", new MemoryUsageGaugeSet());
    // metricRegistry.register("threads", new ThreadStatesGaugeSet());
  }

//  private JmxReporter.Builder jmxReporterBuilder() {
//    return JmxReporter.forRegistry(metricRegistry);
//  }

//  private GraphiteReporter.Builder graphiteReporterBuilder() {
//    return GraphiteReporter
//        .forRegistry(metricRegistry)
//        .convertRatesTo(TimeUnit.SECONDS)
//        .convertDurationsTo(TimeUnit.MILLISECONDS)
//        .filter(MetricFilter.ALL)
//        .prefixedWith(graphiteConfiguration.getPrefix());
//  }

  private Graphite newGraphite() {
    return new Graphite(new InetSocketAddress(graphiteConfiguration.getHost(), graphiteConfiguration.getPort()));
  }

  void setGraphiteConfiguration(GraphiteConfiguration graphiteConfiguration) {
    this.graphiteConfiguration = graphiteConfiguration;
  }

//  void setMetricRegistry(MetricRegistry metricRegistry) {
//    this.metricRegistry = metricRegistry;
//  }

}
