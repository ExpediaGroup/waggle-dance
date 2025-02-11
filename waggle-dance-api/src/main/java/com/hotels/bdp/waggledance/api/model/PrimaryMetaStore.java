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
package com.hotels.bdp.waggledance.api.model;

import java.util.Arrays;
import java.util.List;

import javax.validation.constraints.NotNull;

import lombok.NoArgsConstructor;

@NoArgsConstructor
public class PrimaryMetaStore extends AbstractMetaStore {

  private static final String EMPTY_PREFIX = "";

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

  public PrimaryMetaStore(
          String name,
          String remoteMetaStoreUris,
          String databasePrefix,
          AccessControlType accessControlType,
          String... writableDatabaseWhitelist) {
    this(name, remoteMetaStoreUris, databasePrefix, accessControlType, Arrays.asList(writableDatabaseWhitelist));
  }

  public PrimaryMetaStore(
          String name,
          String remoteMetaStoreUris,
          String databasePrefix,
          AccessControlType accessControlType,
          List<String> writableDatabaseWhitelist) {
    super(name, remoteMetaStoreUris, databasePrefix, accessControlType, writableDatabaseWhitelist);
  }

  @Override
  public FederationType getFederationType() {
    return FederationType.PRIMARY;
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
