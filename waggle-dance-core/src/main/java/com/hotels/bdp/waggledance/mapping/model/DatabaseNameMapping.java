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
package com.hotels.bdp.waggledance.mapping.model;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.log4j.Log4j2;

import com.google.common.collect.BiMap;

@Log4j2
public class DatabaseNameMapping extends MetaStoreMappingDecorator {

  private final Map<String, String> inbound;
  private final Map<String, String> outbound;

  public DatabaseNameMapping(MetaStoreMapping metaStoreMapping, BiMap<String, String> databaseNameMap) {
    super(metaStoreMapping);
    if (databaseNameMap != null && !databaseNameMap.isEmpty()) {
      inbound = new HashMap<>(databaseNameMap.inverse());
      outbound = new HashMap<>(databaseNameMap);
    } else {
      inbound = Collections.emptyMap();
      outbound = Collections.emptyMap();
    }
  }

  @Override
  public String transformOutboundDatabaseName(String databaseName) {
    return transformOutboundDatabaseNameMultiple(databaseName).get(0);
  }

  @Override
  public List<String> transformOutboundDatabaseNameMultiple(String databaseName) {
    List<String> results = new ArrayList<>();
    results.addAll(super.transformOutboundDatabaseNameMultiple(databaseName));
    if (outbound.containsKey(databaseName)) {
      String result = outbound.get(databaseName);
      List<String> databases = super.transformOutboundDatabaseNameMultiple(result);
      log.debug("transformOutboundDatabaseName '{}' to '{}'", databaseName, databases);
      results.addAll(databases);
    }
    return results;
  }

  @Override
  public String transformInboundDatabaseName(String databaseName) {
    String newDatabaseName = super.transformInboundDatabaseName(databaseName);
    String result = inbound.getOrDefault(newDatabaseName, newDatabaseName);
    log.debug("transformInboundDatabaseName '{}' to '{}'", databaseName, result);
    return result;
  }

}
