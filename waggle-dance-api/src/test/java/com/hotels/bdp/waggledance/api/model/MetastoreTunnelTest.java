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
package com.hotels.bdp.waggledance.api.model;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Set;

import javax.validation.ConstraintViolation;

import org.hibernate.validator.HibernateValidator;
import org.junit.Before;
import org.junit.Test;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;

public class MetastoreTunnelTest {

  private final LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
  private final MetastoreTunnel tunnel = new MetastoreTunnel();

  @Before
  public void before() {
    validator.setProviderClass(HibernateValidator.class);
    validator.afterPropertiesSet();

    tunnel.setKnownHosts("knownHosts");
    tunnel.setPrivateKeys("privateKey");
    tunnel.setRoute("hostA -> hostB");
    tunnel.setTimeout(123);
  }

  @Test
  public void typical() {
    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(0));
  }

  @Test
  public void infiniteTimeout() {
    tunnel.setTimeout(0);

    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(0));
  }

  @Test
  public void portTooHigh() {
    tunnel.setPort(65536);

    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void portTooLow() {
    tunnel.setPort(0);

    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullRoute() {
    tunnel.setRoute(null);

    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void emptyRoute() {
    tunnel.setRoute("");

    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void blankRoute() {
    tunnel.setRoute(" ");

    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullKnownHosts() {
    tunnel.setKnownHosts(null);

    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void emptyKnownHosts() {
    tunnel.setKnownHosts("");

    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void blankKnownHosts() {
    tunnel.setKnownHosts(" ");

    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullPrivateKey() {
    tunnel.setPrivateKeys(null);

    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void emptyPrivateKey() {
    tunnel.setPrivateKeys("");

    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void blankPrivateKey() {
    tunnel.setPrivateKeys(" ");

    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void negativeTunnel() {
    tunnel.setTimeout(-1);

    Set<ConstraintViolation<MetastoreTunnel>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

}
