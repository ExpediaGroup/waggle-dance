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
package com.hotels.bdp.waggledance.context;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.SchedulingConfigurer;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;

import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import com.hotels.bdp.waggledance.mapping.service.impl.PollingFederationService;

@Configuration
@EnableScheduling
public class ScheduledBeans implements SchedulingConfigurer {

  private final WaggleDanceConfiguration waggleDanceConfiguration;

  private final PollingFederationService pollingFederationService;

  @Autowired
  public ScheduledBeans(
      WaggleDanceConfiguration waggleDanceConfiguration,
      PollingFederationService pollingFederationService) {
    this.waggleDanceConfiguration = waggleDanceConfiguration;
    this.pollingFederationService = pollingFederationService;
  }

  @Override
  public void configureTasks(ScheduledTaskRegistrar taskRegistrar) {
    Runnable task = pollingFederationService::poll;
    long delay = waggleDanceConfiguration
        .getStatusPollingDelayTimeUnit()
        .toMillis(waggleDanceConfiguration.getStatusPollingDelay());
    taskRegistrar.addFixedDelayTask(task, delay);
  }

}
