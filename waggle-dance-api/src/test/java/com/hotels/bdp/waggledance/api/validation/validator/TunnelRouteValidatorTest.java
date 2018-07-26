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
package com.hotels.bdp.waggledance.api.validation.validator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.validation.ConstraintValidatorContext;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.validation.constraint.TunnelRoute;

@RunWith(MockitoJUnitRunner.class)
public class TunnelRouteValidatorTest {

  private @Mock TunnelRoute constraint;
  private @Mock ConstraintValidatorContext constraintValidatorContext;

  private final TunnelRouteValidator validator = new TunnelRouteValidator();

  @Before
  public void init() {
    validator.initialize(constraint);
  }

  @Test
  public void validNull() {
    assertTrue(validator.isValid(null, constraintValidatorContext));
  }

  @Test
  public void validEmpty() {
    assertTrue(validator.isValid("", constraintValidatorContext));
  }

  @Test
  public void validIpAddress() {
    assertTrue(validator.isValid("127.0.0.1", constraintValidatorContext));
  }

  @Test
  public void validMultiIpAddress() {
    assertTrue(validator.isValid("127.0.0.1 -> 127.0.0.2", constraintValidatorContext));
  }

  @Test
  public void validIpAddressWithUsername() {
    assertTrue(validator.isValid("user@127.0.0.1", constraintValidatorContext));
  }

  @Test
  public void validMultiIpAddressWithUsername() {
    assertTrue(validator.isValid("user@127.0.0.1 -> other@17.0.0.2", constraintValidatorContext));
  }

  @Test
  public void validHostnameWithDomain() {
    assertTrue(validator.isValid("my-host.com", constraintValidatorContext));
  }

  @Test
  public void validHostnameWithDomainWithUsername() {
    assertTrue(validator.isValid("user@my-host.com", constraintValidatorContext));
  }

  @Test
  public void validHostnameOnly() {
    assertTrue(validator.isValid("my-host", constraintValidatorContext));
  }

  @Test
  public void validHostnameOnlyWithUsername() {
    assertTrue(validator.isValid("user@my-host", constraintValidatorContext));
  }

  @Test
  public void invalid() {
    assertFalse(validator.isValid("@", constraintValidatorContext));
  }

  @Test
  public void invalidIpAddressMissingUsername() {
    assertFalse(validator.isValid("@127.0.0.1", constraintValidatorContext));
  }

  @Test
  public void invalidHostnameWithDomainMissingUsername() {
    assertFalse(validator.isValid("@my-host.com", constraintValidatorContext));
  }

  @Test
  public void invalidHostnameMissingUsername() {
    assertFalse(validator.isValid("@my-host", constraintValidatorContext));
  }

  @Test
  public void invalidIpAddressInSecondHost() {
    assertFalse(validator.isValid("user@127.0.0.1 -> .0.0.1", constraintValidatorContext));
  }

  @Test
  public void invalidSecondHost() {
    assertFalse(validator.isValid("user@my-host.com -> +", constraintValidatorContext));
  }

  @Test
  public void invalidHostSeparator() {
    assertFalse(validator.isValid("user@my-host - anotherhost.com", constraintValidatorContext));
  }
}
