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
package com.hotels.bdp.waggledance.rest.endpoint;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotNull;
import javax.xml.bind.ValidationException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import com.hotels.bdp.waggledance.api.ValidationError;
import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;

@RestController
@RequestMapping("/api/admin/federations")
public class FederationsAdminController {

  private final FederationService federationService;

  @Autowired
  public FederationsAdminController(@Qualifier("populateStatusFederationService") FederationService federationService) {
    this.federationService = federationService;
  }

  @RequestMapping(method = RequestMethod.GET)
  @ResponseBody
  public List<AbstractMetaStore> federations() {
    return federationService.getAll();
  }

  @RequestMapping(method = RequestMethod.GET, path = "/{name}")
  @ResponseBody
  public AbstractMetaStore read(@NotNull @PathVariable String name) {
    return federationService.get(name);
  }

  @RequestMapping(method = RequestMethod.POST)
  public void add(@Validated @RequestBody AbstractMetaStore federatedMetaStore) {
    federationService.register(federatedMetaStore);
  }

  @RequestMapping(method = RequestMethod.DELETE, path = "/{name}")
  public void remove(@NotNull @PathVariable String name) {
    federationService.unregister(name);
  }

  @ExceptionHandler(ValidationException.class)
  @ResponseStatus(value = HttpStatus.BAD_REQUEST)
  public ValidationError handleValidationException(HttpServletRequest req, ValidationException exception) {
    return ValidationError.builder().error(exception.getMessage()).build();
  }

  @ExceptionHandler(MethodArgumentNotValidException.class)
  @ResponseStatus(value = HttpStatus.BAD_REQUEST)
  public ValidationError handleException(HttpServletRequest req, MethodArgumentNotValidException exception) {
    return ValidationError.builder(exception.getBindingResult()).build();
  }

}
