/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.validation.Errors;
import org.springframework.validation.ObjectError;

@RunWith(MockitoJUnitRunner.class)
public class ValidationErrorTest {

  private @Mock Errors errors;
  private @Mock ValidationError validationError;

  @Test
  public void builderTypical() {
    ValidationError result = ValidationError.builder().build();
    assertThat(result.getErrorMessage(), is("Validation failed"));
    assertThat(result.getErrors().size(), is(0));
  }

  @Test
  public void builderErrors() {
    ValidationError result = ValidationError.builder(errors).build();
    assertThat(result.getErrorMessage(), is("Validation failed: 0 error(s)"));
    assertThat(result.getErrors().size(), is(0));
  }

  @Test
  public void multipleBuilderErrors() {
    ObjectError objectError1 = new ObjectError("errorOne", "Description one");
    ObjectError objectError2 = new ObjectError("errorTwo", "Description two");

    List<ObjectError> objectErrors = Arrays.asList(objectError1, objectError2);

    when(errors.getErrorCount()).thenReturn(objectErrors.size());
    when(errors.getAllErrors()).thenReturn(objectErrors);

    ValidationError result = ValidationError.builder(errors).build();

    List<String> expected = Arrays.asList(objectError1.getDefaultMessage(), objectError2.getDefaultMessage());

    assertThat(result.getErrors(), is(expected));
    assertThat(result.getErrorMessage(), is("Validation failed: 2 error(s)"));
  }

  @Test
  public void addValidationError() {
    String errorMessage = "Another error";
    ValidationError validationError = ValidationError.builder().build();
    validationError.addValidationError(errorMessage);

    List<String> expectedErrors = Collections.singletonList(errorMessage);

    assertThat(validationError.getErrorMessage(), is("Validation failed"));
    assertThat(validationError.getErrors(), is(expectedErrors));
  }

}
