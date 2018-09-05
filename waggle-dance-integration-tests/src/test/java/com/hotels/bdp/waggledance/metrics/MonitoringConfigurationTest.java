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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import com.hotels.bdp.waggledance.context.CommonBeans;
import com.hotels.bdp.waggledance.mapping.service.PrefixNamingStrategy;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.graphite.GraphiteMeterRegistry;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Rule;
import org.junit.Test;

import com.codahale.metrics.MetricRegistry;

import com.hotels.bdp.waggledance.conf.GraphiteConfiguration;
import com.hotels.bdp.waggledance.junit.ServerSocketRule;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { MonitoringConfigurationTestContext.class, MonitoringConfiguration.class })
public class MonitoringConfigurationTest {

  private @Autowired MeterRegistry meterRegistry;

  @Test
  public void meterRegistry() {
    assertThat(meterRegistry, is(instanceOf(GraphiteMeterRegistry.class)));
  }
}
