/**
 * Copyright (C) 2016-2023 Expedia, Inc.
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

import java.util.List;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;

public interface FederatedMetaStoreStorage {

  void insert(AbstractMetaStore federatedMetaStore);

  AbstractMetaStore delete(String name);

  List<AbstractMetaStore> getAll();

  AbstractMetaStore get(String name);

  void update(AbstractMetaStore oldMetaStore, AbstractMetaStore newMetaStore);

  void saveFederation();
}
