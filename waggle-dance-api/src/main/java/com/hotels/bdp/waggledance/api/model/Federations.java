/**
 * Copyright (C) 2016-2017 Expedia, Inc.
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
package com.hotels.bdp.waggledance.api.model;

import java.util.List;

import javax.validation.Valid;

public class Federations {

  private @Valid PrimaryMetaStore primaryMetaStore;
  private @Valid List<FederatedMetaStore> federatedMetaStores;

  public Federations() {
  }

  public Federations(PrimaryMetaStore primaryMetaStore, List<FederatedMetaStore> federatedMetaStores) {
    this.primaryMetaStore = primaryMetaStore;
    this.federatedMetaStores = federatedMetaStores;
  }

  public PrimaryMetaStore getPrimaryMetaStore() {
    return primaryMetaStore;
  }

  public void setPrimaryMetaStore(PrimaryMetaStore primaryMetaStore) {
    this.primaryMetaStore = primaryMetaStore;
  }

  public List<FederatedMetaStore> getFederatedMetaStores() {
    return federatedMetaStores;
  }

  public void setFederatedMetaStores(List<FederatedMetaStore> federatedMetaStores) {
    this.federatedMetaStores = federatedMetaStores;
  }
}
