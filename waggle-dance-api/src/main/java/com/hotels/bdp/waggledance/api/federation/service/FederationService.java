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
package com.hotels.bdp.waggledance.api.federation.service;

import java.util.List;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;

public interface FederationService {

  void register(@NotNull @Valid AbstractMetaStore federatedMetaStore);

  void update(@NotNull @Valid AbstractMetaStore oldMetaStore, @NotNull @Valid AbstractMetaStore newMetaStore);

  void unregister(@NotNull String name);

  AbstractMetaStore get(@NotNull String name);

  List<AbstractMetaStore> getAll();

}
