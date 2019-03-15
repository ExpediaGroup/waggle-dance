/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.waggledance.mapping.service.impl;

import java.io.IOException;
import java.util.List;

import javax.validation.constraints.NotNull;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.metrics.CurrentMonitoredMetaStoreHolder;

public class MonitoredDatabaseMappingService implements MappingEventListener {

  private final MappingEventListener wrapped;

  public MonitoredDatabaseMappingService(MappingEventListener wrapped) {
    this.wrapped = wrapped;
  }

  @Override
  public DatabaseMapping primaryDatabaseMapping() {
    DatabaseMapping primaryDatabaseMapping = wrapped.primaryDatabaseMapping();
    CurrentMonitoredMetaStoreHolder.monitorMetastore(primaryDatabaseMapping.getMetastoreMappingName());
    return primaryDatabaseMapping;
  }

  @Override
  public DatabaseMapping databaseMapping(@NotNull String databaseName) {
    DatabaseMapping databaseMapping = wrapped.databaseMapping(databaseName);
    CurrentMonitoredMetaStoreHolder.monitorMetastore(databaseMapping.getMetastoreMappingName());
    return databaseMapping;
  }

  @Override
  public PanopticOperationHandler getPanopticOperationHandler() {
    PanopticOperationHandler handler = wrapped.getPanopticOperationHandler();
    CurrentMonitoredMetaStoreHolder.monitorMetastore();
    return handler;
  }

  @Override
  public List<DatabaseMapping> getDatabaseMappings() {
    return wrapped.getDatabaseMappings();
  }

  @Override
  public void close() throws IOException {
    wrapped.close();
  }

  @Override
  public void onRegister(AbstractMetaStore federatedMetaStore) {
    wrapped.onRegister(federatedMetaStore);
  }

  @Override
  public void onUnregister(AbstractMetaStore federatedMetaStore) {
    wrapped.onUnregister(federatedMetaStore);
  }

  @Override
  public void onUpdate(AbstractMetaStore oldMetaStore, AbstractMetaStore newMetaStore) {
    wrapped.onUpdate(oldMetaStore, newMetaStore);
  }
}
