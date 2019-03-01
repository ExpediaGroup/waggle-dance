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
package com.hotels.bdp.waggledance.mapping.service.requests;

import static com.hotels.bdp.waggledance.mapping.service.DatabaseMappingUtils.PREFIXED_RESOLUTION_TYPE;
import static com.hotels.bdp.waggledance.mapping.service.DatabaseMappingUtils.isWhitelisted;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.thrift.TException;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.util.Whitelist;

public class GetAllDatabasesByPatternRequest implements Callable<List<?>> {

  private final String pattern;
  private final Map<String, Whitelist> mappedDbByPrefix;
  private final String resolutionType;
  private final Map<String, DatabaseMapping> mappingsByDatabaseName;
  private final DatabaseMapping mapping;
  private final DatabaseMapping primaryMapping;

  public GetAllDatabasesByPatternRequest(DatabaseMapping mapping, String pattern,
                                         Map<String, Whitelist> mappedDbByPrefix,
                                         Map<String, DatabaseMapping> mappingsByDatabaseName,
                                         DatabaseMapping primaryMapping, String resolutionType) {
    this.mapping = mapping;
    this.pattern = pattern;
    this.mappedDbByPrefix = mappedDbByPrefix;
    this.mappingsByDatabaseName = mappingsByDatabaseName;
    this.resolutionType = resolutionType;
    this.primaryMapping = primaryMapping;
  }

  @Override
  public List<?> call() throws TException {
    List<String> databases = mapping.getClient().get_databases(pattern);
    List<String> mappedDatabases = new ArrayList<>();
    for (String database : databases) {
      boolean shouldBeAdded = getConditionForResolutionType(database);
      if (shouldBeAdded) {
        mappedDatabases.add(mapping.transformOutboundDatabaseName(database));
      }
    }
    return mappedDatabases;
  }

  private boolean getConditionForResolutionType(String database) {
    if (resolutionType.equals(PREFIXED_RESOLUTION_TYPE)) {
      return isWhitelisted(mapping.getDatabasePrefix(), database, mappedDbByPrefix);
    } else {
      boolean isPrimaryDatabase = mapping.equals(primaryMapping);
      boolean isMapped = mappingsByDatabaseName.keySet().contains(database);
      return isPrimaryDatabase || isMapped;
    }
  }
}
