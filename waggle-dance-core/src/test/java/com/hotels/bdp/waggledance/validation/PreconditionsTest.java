/**
 * Copyright (C) 2016-2017 Expedia Inc.
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
package com.hotels.bdp.waggledance.validation;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import javax.validation.ValidationException;

import org.junit.Test;

public class PreconditionsTest {

  @Test
  public void checkNotNull() {
    Object obj = new Object();
    assertThat(Preconditions.checkNotNull(obj, "message"), is(obj));
  }

  @Test(expected = ValidationException.class)
  public void checkNotNullFails() {
    Preconditions.checkNotNull(null, "message");
  }

  @Test
  public void checkIsTrue() {
    assertThat(Preconditions.checkIsTrue(true, "message"), is(true));
  }

  @Test(expected = ValidationException.class)
  public void checkIsTrueFails() {
    Preconditions.checkIsTrue(false, "message");
  }

}
