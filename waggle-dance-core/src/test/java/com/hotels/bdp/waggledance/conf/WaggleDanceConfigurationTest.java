/**
 * Copyright (C) 2016-2018 Expedia, Inc.
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
import static org.junit.Assert.assertThat;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.validation.ConstraintViolation;

import org.hibernate.validator.HibernateValidator;
import org.junit.Before;
import org.junit.Test;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.waggledance.api.model.DatabaseResolution;

public class WaggleDanceConfigurationTest {

  private final LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
  private final WaggleDanceConfiguration waggleDanceConfiguration = new WaggleDanceConfiguration();

  @Before
  public void before() {
    validator.setProviderClass(HibernateValidator.class);
    validator.afterPropertiesSet();

    waggleDanceConfiguration.setPort(123);
    waggleDanceConfiguration.setVerbose(true);
    waggleDanceConfiguration.setDisconnectConnectionDelay(15);
    waggleDanceConfiguration.setDisconnectTimeUnit(TimeUnit.SECONDS);
    waggleDanceConfiguration
        .setConfigurationProperties(ImmutableMap.<String, String> builder().put("prop1", "val1").build());
  }

  @Test
  public void typical() {
    Set<ConstraintViolation<WaggleDanceConfiguration>> violations = validator.validate(waggleDanceConfiguration);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void nullPort() {
    waggleDanceConfiguration.setPort(null);
    Set<ConstraintViolation<WaggleDanceConfiguration>> violations = validator.validate(waggleDanceConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void zeroPort() {
    waggleDanceConfiguration.setPort(0);
    Set<ConstraintViolation<WaggleDanceConfiguration>> violations = validator.validate(waggleDanceConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void negativePort() {
    waggleDanceConfiguration.setPort(-1);
    Set<ConstraintViolation<WaggleDanceConfiguration>> violations = validator.validate(waggleDanceConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void zeroDisconnectConnectionDelay() {
    waggleDanceConfiguration.setDisconnectConnectionDelay(0);
    Set<ConstraintViolation<WaggleDanceConfiguration>> violations = validator.validate(waggleDanceConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void negativeDisconnectConnectionDelay() {
    waggleDanceConfiguration.setDisconnectConnectionDelay(-1);
    Set<ConstraintViolation<WaggleDanceConfiguration>> violations = validator.validate(waggleDanceConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullDisconnectTimeUnit() {
    waggleDanceConfiguration.setDisconnectTimeUnit(null);
    Set<ConstraintViolation<WaggleDanceConfiguration>> violations = validator.validate(waggleDanceConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullConfigurationProperties() {
    waggleDanceConfiguration.setConfigurationProperties(null);
    Set<ConstraintViolation<WaggleDanceConfiguration>> violations = validator.validate(waggleDanceConfiguration);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void emptyConfigurationProperties() {
    waggleDanceConfiguration.setConfigurationProperties(ImmutableMap.<String, String> of());
    Set<ConstraintViolation<WaggleDanceConfiguration>> violations = validator.validate(waggleDanceConfiguration);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void nullDatabaseResolution() {
    waggleDanceConfiguration.setDatabaseResolution(null);
    Set<ConstraintViolation<WaggleDanceConfiguration>> violations = validator.validate(waggleDanceConfiguration);
    assertThat(violations.size(), is(1));
  }

  @Test
  public void setterGetterDatabaseResolution() {
    waggleDanceConfiguration.setDatabaseResolution(DatabaseResolution.PREFIXED);
    assertThat(waggleDanceConfiguration.getDatabaseResolution(), is(DatabaseResolution.PREFIXED));
  }

  @Test
  public void setterGetterThriftServerStopTimeoutValInSeconds() {
    int timeout = 10;
    waggleDanceConfiguration.setThriftServerStopTimeoutValInSeconds(timeout);
    assertThat(waggleDanceConfiguration.getThriftServerStopTimeoutValInSeconds(), is(timeout));
  }

  @Test
  public void setterGetterThriftServerRequestTimeout() {
    int timeout = 10;
    waggleDanceConfiguration.setThriftServerRequestTimeout(timeout);
    assertThat(waggleDanceConfiguration.getThriftServerRequestTimeout(), is(timeout));
  }

  @Test
  public void setterGetterThriftServerRequestTimeoutUnit() {
    TimeUnit timeUnit = TimeUnit.NANOSECONDS;
    waggleDanceConfiguration.setThriftServerRequestTimeoutUnit(timeUnit);
    assertThat(waggleDanceConfiguration.getThriftServerRequestTimeoutUnit(), is(timeUnit));
  }
}
