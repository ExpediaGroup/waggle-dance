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
package com.hotels.bdp.waggledance.context;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.common.collect.ImmutableMap;

import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;

@Configuration
public class CommonBeansTestContext {

  static final String PROP_1 = "prop_1";
  static final String VAL_1 = "val_1";
  static final String PROP_2 = "prop_2";
  static final String VAL_2 = "val_2";

  @Bean
  WaggleDanceConfiguration waggleDanceConfiguration() {
    WaggleDanceConfiguration conf = new WaggleDanceConfiguration();
    conf.setConfigurationProperties(
        ImmutableMap.<String, String> builder().put(PROP_1, VAL_1).put(PROP_2, VAL_2).build());
    return conf;
  }
}
