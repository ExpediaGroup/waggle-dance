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

import com.hotels.hcommon.ssh.SshSettings;
import com.hotels.hcommon.ssh.SshSettings.Builder;

public class MetastoreTunnelTest {

  private final LocalValidatorFactoryBean validator = new LocalValidatorFactoryBean();
  private Builder tunnelBuilder;

  @Before
  public void before() {
    validator.setProviderClass(HibernateValidator.class);
    validator.afterPropertiesSet();
    tunnelBuilder = SshSettings
        .builder()
        .withKnownHosts("knownHosts")
        .withPrivateKeys("privateKey")
        .withRoute("hostA -> hostB")
        .withSessionTimeout(123);
  }

  @Test
  public void typical() {
    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnelBuilder.build());
    assertThat(violations.size(), is(0));
  }

  @Test
  public void infiniteTimeout() {
    SshSettings tunnel = tunnelBuilder.withSessionTimeout(0).build();
    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void portTooHigh() {
    SshSettings tunnel = tunnelBuilder.withSshPort(65536).build();
    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void portTooLow() {
    SshSettings tunnel = tunnelBuilder.withSshPort(0).build();
    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullRoute() {
    SshSettings tunnel = tunnelBuilder.withRoute(null).build();
    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void emptyRoute() {
    SshSettings tunnel = tunnelBuilder.withRoute("").build();

    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void blankRoute() {
    SshSettings tunnel = tunnelBuilder.withRoute(" ").build();

    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullKnownHosts() {
    SshSettings tunnel = tunnelBuilder.withKnownHosts(null).build();

    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void emptyKnownHosts() {
    SshSettings tunnel = tunnelBuilder.withKnownHosts("").build();

    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void blankKnownHosts() {
    SshSettings tunnel = tunnelBuilder.withKnownHosts(" ").build();

    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void nullPrivateKey() {
    SshSettings tunnel = tunnelBuilder.withPrivateKeys(null).build();

    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void emptyPrivateKey() {
    SshSettings tunnel = tunnelBuilder.withPrivateKeys("").build();

    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void blankPrivateKey() {
    SshSettings tunnel = tunnelBuilder.withPrivateKeys(" ").build();

    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void negativeTimeout() {
    SshSettings tunnel = tunnelBuilder.withSessionTimeout(-1).build();
    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void strictHostKeyCheckingSetToYes() {
    SshSettings tunnel = tunnelBuilder.withStrictHostKeyChecking(true).build();
    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void strictHostKeyCheckingSetToNo() {
    SshSettings tunnel = tunnelBuilder.withStrictHostKeyChecking(false).build();
    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);
    assertThat(violations.size(), is(0));
  }

  @Test
  public void strictHostKeyCheckingSetToIncorrectValue() {
    SshSettings tunnel = tunnelBuilder.withStrictHostKeyChecking((Boolean) null).build();

    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(1));
  }

  @Test
  public void strictHostKeyCheckingDefaultsToYes() {
    SshSettings tunnel = tunnelBuilder.build();
    assertThat(tunnel.isStrictHostKeyChecking(), is(true));

    Set<ConstraintViolation<SshSettings>> violations = validator.validate(tunnel);

    assertThat(violations.size(), is(0));
  }
}
