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
package com.hotels.bdp.waggledance.mapping.service;

import java.io.Closeable;

import javax.validation.constraints.NotNull;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;

public interface DatabaseMappingService extends Closeable {

  /**
   * @return the {@link DatabaseMapping} that maps to the primary metastore
   */
  DatabaseMapping primaryDatabaseMapping();

  /**
   * @param databaseName given database name
   * @return the {@link DatabaseMapping} that maps to the given databaseName
   */
  DatabaseMapping databaseMapping(@NotNull String databaseName);

  PanopticOperationHandler getPanopticOperationHandler();

}
