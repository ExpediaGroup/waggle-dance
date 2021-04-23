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
package com.hotels.bdp.waggledance.api;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;

import org.junit.Test;

public class WaggleDanceExceptionTest {

  private String message = "message";
  private Throwable cause = new Throwable();

  @Test
  public void messageException() {
    WaggleDanceException exception = new WaggleDanceException(message);
    assertThat(exception.getMessage(), is(message));
    assertNull(exception.getCause());
  }

  @Test
  public void messageAndCauseException() {
    WaggleDanceException exception = new WaggleDanceException(message, cause);
    assertThat(exception.getMessage(), is(message));
    assertThat(exception.getCause(), is(cause));
  }

}
