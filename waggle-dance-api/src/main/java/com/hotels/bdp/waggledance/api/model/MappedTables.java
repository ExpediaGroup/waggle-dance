/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

public class MappedTables {
  @NotBlank private String database;
  @NotEmpty private List<String> mappedTables;

  public MappedTables() {
  }

  public MappedTables(String database, List<String> mappedTables) {
    this.database = database;
    this.mappedTables = mappedTables;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public List<String> getMappedTables() {
    return mappedTables;
  }

  public void setMappedTables(List<String> mappedTables) {
    this.mappedTables = mappedTables;
  }
}
