/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseNameMapping extends MetaStoreMappingDecorator {

  private final static Logger log = LoggerFactory.getLogger(DatabaseNameMapping.class);

  private final Map<String, String> inbound = new HashMap<>();
  private final Map<String, String> outbound = new HashMap<>();

  public DatabaseNameMapping(MetaStoreMapping metaStoreMapping) {
    super(metaStoreMapping);
    // TODO PD config :P Need to get this from config loaded into: metaStoreMapping.getDatabaseNameMapping()
    inbound.put("foo", "bar");

    outbound.put("bar", "foo2");
    // TODO PD: What if you have multiple values? we should validate.
    // databaseNameMapping.put("foo2", "bar");
  }

  @Override
  public String transformOutboundDatabaseName(String databaseName) {
    String result = super.transformOutboundDatabaseName(outbound.getOrDefault(databaseName, databaseName));
    log.info("transformOutboundDatabaseName '" + databaseName + "' to '" + result + "'");
    return result;
  }

  @Override
  public String transformInboundDatabaseName(String databaseName) {
    String newDatabaseName = super.transformInboundDatabaseName(databaseName);
    String result = inbound.getOrDefault(newDatabaseName, newDatabaseName);
    log.info("transformInboundDatabaseName '" + databaseName + "' to '" + result + "'");
    return result;
  }

}
