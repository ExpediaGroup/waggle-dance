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
package com.hotels.bdp.waggledance.mapping.service.requests;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.thrift.TException;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.service.DatabaseMappingUtils;
import com.hotels.bdp.waggledance.util.Whitelist;

public class GetAllDatabasesRequest implements Callable<List<?>> {

  private final DatabaseMapping mapping;
  private final Map<String, Whitelist> mappedDbByPrefix;

  public GetAllDatabasesRequest(DatabaseMapping mapping, Map<String, Whitelist> mappedDbByPrefix) {
    this.mapping = mapping;
    this.mappedDbByPrefix = mappedDbByPrefix;
  }

  @Override
  public List<String> call() throws TException {
    List<String> databases = mapping.getClient().get_all_databases();
    return DatabaseMappingUtils.getMappedWhitelistedDatabases(databases, mapping, mappedDbByPrefix);
  }
}
