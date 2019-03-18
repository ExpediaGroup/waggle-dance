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

import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.federation.service.FederationStatusService;

@Configuration
@EnableWebMvc
@ComponentScan(basePackages = { "com.hotels.bdp.waggledance.rest.endpoint" })
@WebAppConfiguration
public class TestContext {

  @Bean
  public FederationService federationService() {
    return Mockito.mock(FederationService.class);
  }

  @Bean
  public FederationStatusService federationStatusService() {
    return Mockito.mock(FederationStatusService.class);
  }

}
