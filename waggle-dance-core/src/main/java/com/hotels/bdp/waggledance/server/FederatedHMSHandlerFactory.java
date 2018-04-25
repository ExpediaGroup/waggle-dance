/**
 * Copyright (C) 2016-2017 Expedia Inc.
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
package com.hotels.bdp.waggledance.server;

import org.apache.hadoop.hive.conf.HiveConf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.impl.MonitoredDatabaseMappingService;
import com.hotels.bdp.waggledance.mapping.service.impl.NotifyingFederationService;
import com.hotels.bdp.waggledance.mapping.service.impl.PrefixBasedDatabaseMappingService;
import com.hotels.bdp.waggledance.mapping.service.impl.StaticDatabaseMappingService;

@Component
public class FederatedHMSHandlerFactory {

  private final HiveConf hiveConf;
  private final NotifyingFederationService notifyingFederationService;
  private final MetaStoreMappingFactory metaStoreMappingFactory;
  private final WaggleDanceConfiguration waggleDanceConfiguration;

  @Autowired
  public FederatedHMSHandlerFactory(
      HiveConf hiveConf,
      NotifyingFederationService notifyingFederationService,
      MetaStoreMappingFactory metaStoreMappingFactory,
      WaggleDanceConfiguration waggleDanceConfiguration) {
    this.hiveConf = hiveConf;
    this.notifyingFederationService = notifyingFederationService;
    this.metaStoreMappingFactory = metaStoreMappingFactory;
    this.waggleDanceConfiguration = waggleDanceConfiguration;
  }

  public CloseableIHMSHandler create() {
    MappingEventListener service = createDatabaseMappingService();
    MonitoredDatabaseMappingService monitoredService = new MonitoredDatabaseMappingService(service);
    CloseableIHMSHandler baseHandler = new FederatedHMSHandler(monitoredService, notifyingFederationService);
    HiveConf hiveConf = new HiveConf(this.hiveConf);
    baseHandler.setConf(hiveConf);
    return baseHandler;
  }

  private MappingEventListener createDatabaseMappingService() {
    switch (waggleDanceConfiguration.getDatabaseResolution()) {
    case MANUAL:
      final StaticDatabaseMappingService prefixAvoidingService = new StaticDatabaseMappingService(
          metaStoreMappingFactory, notifyingFederationService.getAll());
      return prefixAvoidingService;

    case PREFIXED:
      final PrefixBasedDatabaseMappingService prefixBasedService = new PrefixBasedDatabaseMappingService(
          metaStoreMappingFactory, notifyingFederationService.getAll());
      return prefixBasedService;

    default:
      throw new WaggleDanceException("Cannot instantiate databaseMappingService for prefixType '"
          + waggleDanceConfiguration.getDatabaseResolution()
          + "'");
    }
  }
}
