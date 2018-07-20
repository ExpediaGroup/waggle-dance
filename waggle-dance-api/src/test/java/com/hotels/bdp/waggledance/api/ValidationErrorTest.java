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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.validation.Errors;
import org.springframework.validation.ObjectError;

@RunWith(MockitoJUnitRunner.class)
public class ValidationErrorTest {

  private @Mock Errors errors;
  private @Mock ValidationError validationError;

  @Test
  public void testBuilder() {
    ValidationError result = ValidationError.builder().build();
    assertThat(result.getErrorMessage(), is("Validation failed"));
    assertThat(result.getErrors().size(), is(0));
  }

  @Test
  public void testBuilderErrors() {
    ValidationError result = ValidationError.builder(errors).build();
    assertThat(result.getErrorMessage(), is("Validation failed: 0 error(s)"));
    assertThat(result.getErrors().size(), is(0));
  }

  @Test
  public void testMultipleBuilderErrors() {
    List<ObjectError> objectError = new ArrayList<>();
    objectError.add(new ObjectError("errorOne", "Description one"));
    objectError.add(new ObjectError("errorTwo", "Description two"));

    when(errors.getErrorCount()).thenReturn(objectError.size());
    when(errors.getAllErrors()).thenReturn(objectError);

    List<String> expected = new ArrayList<>();
    expected.add("Description one");
    expected.add("Description two");

    ValidationError result = ValidationError.builder(errors).build();

    assertThat(result.getErrors(), is(expected));
    assertThat(result.getErrorMessage(), is("Validation failed: 2 error(s)"));
  }

  @Test
  public void testAddValidationError() {
    ValidationError validationError = ValidationError.builder().build();
    validationError.addValidationError("Another error");

    List<String> expectedErrors = new ArrayList<String>();
    expectedErrors.add("Another error");

    assertThat(validationError.getErrorMessage(), is("Validation failed"));
    assertThat(validationError.getErrors(), is(expectedErrors));
  }

}
