/**
 * Copyright (C) 2007-2017 The Guava Authors and Expedia Inc.
 *
 * This code is based on:
 *
 * https://google.github.io/guava/releases/16.0.1/api/docs/com/google/common/base/Preconditions.html
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/
package com.hotels.bdp.waggledance.validation;

import javax.validation.ValidationException;

public final class Preconditions {

  private Preconditions() {}

  public static <T> T checkNotNull(T object, String message) {
    if (object == null) {
      throw new ValidationException(message);
    }
    return object;
  }

  public static boolean checkIsTrue(boolean condition, String message) {
    if (!condition) {
      throw new ValidationException(message);
    }
    return condition;
  }

}
