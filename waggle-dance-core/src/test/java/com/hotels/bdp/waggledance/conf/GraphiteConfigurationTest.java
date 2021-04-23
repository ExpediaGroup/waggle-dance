/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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
package com.hotels.bdp.waggledance.conf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.validation.ConstraintViolation;

import org.hibernate.validator.HibernateValidator;
import org.junit.Before;
import org.junit.Test;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

public class GraphiteConfigurationTest {

  private final LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
  private final GraphiteConfiguration graphiteConfiguration = new GraphiteConfiguration();

  @Before
  public void before() {
    validator.setProviderClass(HibernateValidator.class);
    validator.afterPropertiesSet();

    graphiteConfiguration.setPort(100);
    graphiteConfiguration.setHost("host");
    graphiteConfiguration.setPrefix("prefix");
    graphiteConfiguration.setPollInterval(100);
    graphiteConfiguration.setPollIntervalTimeUnit(TimeUnit.MICROSECONDS);
    graphiteConfiguration.init();
  }

  @Test
  public void typical() {
    Set<ConstraintViolation<GraphiteConfiguration>> violations = validator.validate(graphiteConfiguration);
    assertThat(violations.size(), is(0));
    assertTrue(graphiteConfiguration.isEnabled());
    assertThat(graphiteConfiguration.getPort(), is(100));
    assertThat(graphiteConfiguration.getHost(), is("host"));
    assertThat(graphiteConfiguration.getPrefix(), is("prefix"));
    assertThat(graphiteConfiguration.getPollInterval(), is((long) 100));
    assertThat(graphiteConfiguration.getPollIntervalTimeUnit(), is(TimeUnit.MICROSECONDS));
  }

  @Test
  public void zeroPort() {
    graphiteConfiguration.setPort(0);
    Set<ConstraintViolation<GraphiteConfiguration>> violations = validator.validate(graphiteConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void negativePort() {
    graphiteConfiguration.setPort(-1);
    Set<ConstraintViolation<GraphiteConfiguration>> violations = validator.validate(graphiteConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void zeroPollInterval() {
    graphiteConfiguration.setPollInterval(0);
    Set<ConstraintViolation<GraphiteConfiguration>> violations = validator.validate(graphiteConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void negativePollInterval() {
    graphiteConfiguration.setPollInterval(-1);
    Set<ConstraintViolation<GraphiteConfiguration>> violations = validator.validate(graphiteConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullPollIntervalTimeUnit() {
    graphiteConfiguration.setPollIntervalTimeUnit(null);
    Set<ConstraintViolation<GraphiteConfiguration>> violations = validator.validate(graphiteConfiguration);
    assertThat(violations.size(), is(1));
  }

}
