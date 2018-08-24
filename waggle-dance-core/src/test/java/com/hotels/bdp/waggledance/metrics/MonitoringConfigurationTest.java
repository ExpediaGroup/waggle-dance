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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

import com.hotels.bdp.waggledance.conf.GraphiteConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class MonitoringConfigurationTest {

  private final MetricRegistry metricRegistry = new MetricRegistry();
  private final GraphiteConfiguration graphiteConfiguration = new GraphiteConfiguration();

  private final MonitoringConfiguration monitoringConfiguration = new MonitoringConfiguration();

  @Before
  public void setUp() {
    monitoringConfiguration.setMetricRegistry(metricRegistry);
    monitoringConfiguration.setGraphiteConfiguration(graphiteConfiguration);
  }

  @Test
  public void disabledGraphiteConfiguration() {
    monitoringConfiguration.init();
    Set<Closeable> reporters = monitoringConfiguration.getReporters();
    assertThat(reporters.size(), is(1));
    assertThat(reporters.iterator().next(), instanceOf(JmxReporter.class));
  }

  @Test
  public void enabledGraphiteConfiguration() {
    graphiteConfiguration.setHost("host");
    graphiteConfiguration.setPort(42);
    graphiteConfiguration.init();
    monitoringConfiguration.setGraphiteConfiguration(graphiteConfiguration);
    monitoringConfiguration.init();
    Set<Closeable> reporters = monitoringConfiguration.getReporters();
    assertThat(reporters.size(), is(2));

    // if one reporter is JmxReporter, then the other is GraphiteConfiguration
    Iterator<Closeable> iterator = reporters.iterator();
    Closeable firstReporter = iterator.next();
    Closeable secondReporter = iterator.next();
    if (JmxReporter.class.isInstance(firstReporter)) {
      assertThat(secondReporter, instanceOf(GraphiteConfiguration.class));
    } else {
      assertThat(secondReporter, instanceOf(JmxReporter.class));
    }
  }

}
