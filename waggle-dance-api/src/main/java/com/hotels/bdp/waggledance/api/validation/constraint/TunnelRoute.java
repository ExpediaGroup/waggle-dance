/**
 * Copyright (C) 2016-2017 Expedia Inc and hibernate-validator contributors.
 *
 * Based on:
 *
 * https://github.com/hibernate/hibernate-validator/blob/5.2/engine/src/main/java/org/hibernate/validator/constraints/Email.java#L37
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
package com.hotels.bdp.waggledance.api.validation.constraint;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.OverridesAttribute;
import javax.validation.Payload;
import javax.validation.ReportAsSingleViolation;
import javax.validation.constraints.Pattern;

import com.hotels.bdp.waggledance.api.validation.validator.TunnelRouteValidator;

/**
 * Validates the annotated {@link String} is sequence of one or more {@link java.net.URI} separated by the literal
 * {@code ->}, each of which contains an optional username and hostname only.
 * <p>
 * Refer to {@link TunnelRouteValidator} for more details about valid expressions.
 * </p>
 *
 * @see TunnelRouteValidator
 */
@Documented
@Constraint(validatedBy = { TunnelRouteValidator.class })
@Target({ METHOD, FIELD, ANNOTATION_TYPE, CONSTRUCTOR, PARAMETER })
@Retention(RUNTIME)
@ReportAsSingleViolation
@Pattern(regexp = "")
public @interface TunnelRoute {
  String message() default "{com.hotels.bdp.waggledance.api.validation.constraints.TunnelRoute.message}";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};

  /**
   * @return an additional regular expression the annotated string must match. The default is any string ('.*')
   */
  @OverridesAttribute(constraint = Pattern.class, name = "regexp")
  String regexp() default ".*";

  /**
   * @return used in combination with {@link #regexp()} in order to specify a regular expression option
   */
  @OverridesAttribute(constraint = Pattern.class, name = "flags")
  Pattern.Flag[] flags() default {};

}
