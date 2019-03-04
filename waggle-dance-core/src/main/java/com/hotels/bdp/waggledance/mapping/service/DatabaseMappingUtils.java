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
package com.hotels.bdp.waggledance.mapping.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.util.Whitelist;

public class DatabaseMappingUtils {

  public static boolean isWhitelisted(String databasePrefix, String database, Map<String, Whitelist> mappedDbByPrefix) {
    Whitelist whitelist = mappedDbByPrefix.get(databasePrefix);
    if ((whitelist == null) || whitelist.isEmpty()) {
      // Accept everything
      return true;
    }
    return whitelist.contains(database);
  }

  public static List<String> getMappedWhitelistedDatabases(List<String> databases, DatabaseMapping mapping,
                                                           Map<String, Whitelist> mappedDbByPrefix) {
    List<String> mappedDatabases = new ArrayList<>();
    for (String database : databases) {
      if (DatabaseMappingUtils.isWhitelisted(mapping.getDatabasePrefix(), database, mappedDbByPrefix)) {
        mappedDatabases.add(mapping.transformOutboundDatabaseName(database));
      }
    }
    return mappedDatabases;
  }
}
