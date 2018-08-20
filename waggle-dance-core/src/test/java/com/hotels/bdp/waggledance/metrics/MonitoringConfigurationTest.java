package com.hotels.bdp.waggledance.metrics;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

import com.hotels.bdp.waggledance.conf.GraphiteConfiguration;

@RunWith(MockitoJUnitRunner.class)
public class MonitoringConfigurationTest {

  private final MetricRegistry metricRegistry = new MetricRegistry();
  private @Mock GraphiteConfiguration graphiteConfiguration;

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
    when(graphiteConfiguration.isEnabled()).thenReturn(true);
    when(graphiteConfiguration.getHost()).thenReturn("host");
    when(graphiteConfiguration.getPort()).thenReturn(42);
    when(graphiteConfiguration.getPollInterval()).thenReturn((long) 42);
    when(graphiteConfiguration.getPollIntervalTimeUnit()).thenReturn(TimeUnit.NANOSECONDS);
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
