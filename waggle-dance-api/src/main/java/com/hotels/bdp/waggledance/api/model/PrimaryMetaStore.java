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
package com.hotels.bdp.waggledance.api.model;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.validation.constraints.NotNull;

public class PrimaryMetaStore extends AbstractMetaStore {

  private static final String EMPTY_PREFIX = "";
  private @NotNull List<String> mappedDatabases = Collections.emptyList();
  private boolean noMappedDatabases = false;

  public PrimaryMetaStore() {}

  public PrimaryMetaStore(
      String name,
      String remoteMetaStoreUris,
      AccessControlType accessControlType,
      String... writableDatabaseWhitelist) {
    this(name, remoteMetaStoreUris, accessControlType, Arrays.asList(writableDatabaseWhitelist));
  }

  public PrimaryMetaStore(
      String name,
      String remoteMetaStoreUris,
      AccessControlType accessControlType,
      List<String> writableDatabaseWhitelist) {
    super(name, remoteMetaStoreUris, accessControlType, writableDatabaseWhitelist);
  }

  @Override
  public FederationType getFederationType() {
    return FederationType.PRIMARY;
  }

  @Override
  public List<String> getMappedDatabases() {
    return mappedDatabases;
  }

  @Override
  public void setMappedDatabases(List<String> mappedDatabases) {
    noMappedDatabases = mappedDatabases.isEmpty();
    this.mappedDatabases = mappedDatabases;
  }

  @Override
  public boolean shouldHaveNoMappedDatabases() {
    return noMappedDatabases;
  }

  @NotNull
  @Override
  public String getDatabasePrefix() {
    String prefix = super.getDatabasePrefix();
    if (prefix == null) {
      prefix = EMPTY_PREFIX;
    }
    return prefix;
  }

}
