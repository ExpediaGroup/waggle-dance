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
package com.hotels.bdp.waggledance.api;

import java.util.ArrayList;
import java.util.List;

import org.springframework.validation.Errors;
import org.springframework.validation.ObjectError;

import com.fasterxml.jackson.annotation.JsonInclude;

public class ValidationError {

  public static final class ValidationErrorBuilder {

    private final ValidationError error;

    private ValidationErrorBuilder(String globalMessage) {
      error = new ValidationError(globalMessage);
    }

    public ValidationErrorBuilder error(String message) {
      error.addValidationError(message);
      return this;
    }

    public ValidationError build() {
      return error;
    }

  }

  public static ValidationErrorBuilder builder() {
    return new ValidationErrorBuilder("Validation failed");
  }

  public static ValidationErrorBuilder builder(Errors errors) {
    ValidationErrorBuilder builder = new ValidationErrorBuilder(
        "Validation failed: " + errors.getErrorCount() + " error(s)");
    for (ObjectError objectError : errors.getAllErrors()) {
      builder.error(objectError.getDefaultMessage());
    }
    return builder;
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  private final List<String> errors = new ArrayList<>();
  private final String errorMessage;

  private ValidationError(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public void addValidationError(String error) {
    errors.add(error);
  }

  public List<String> getErrors() {
    return errors;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

}
