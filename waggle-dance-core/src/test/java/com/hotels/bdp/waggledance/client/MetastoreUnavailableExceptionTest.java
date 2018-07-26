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
package com.hotels.bdp.waggledance.client;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class MetastoreUnavailableExceptionTest {

  private final String errorMessage = "error";

  @Test
  public void messageError() {
    MetastoreUnavailableException exception = new MetastoreUnavailableException(errorMessage);
    assertThat(exception.getMessage(), is(errorMessage));
    assertNull(exception.getCause());
  }

  @Test
  public void messageAndCauseError() {
    Throwable cause = new Throwable();
    MetastoreUnavailableException exception = new MetastoreUnavailableException(errorMessage, cause);
    assertThat(exception.getMessage(), is(errorMessage));
    assertThat(exception.getCause(), is(cause));
  }

}
