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

public class PrefixMapping extends MetaStoreMappingDecorator {

  public PrefixMapping(MetaStoreMapping metaStoreMapping) {
    super(metaStoreMapping);
  }

  @Override
  public String transformOutboundDatabaseName(String databaseName) {
    return getDatabasePrefix() + super.transformOutboundDatabaseName(databaseName);
  }

  @Override
  public String transformInboundDatabaseName(String databaseName) {
    String result = super.transformInboundDatabaseName(databaseName);
    if (result.startsWith(getDatabasePrefix())) {
      return result.substring(getDatabasePrefix().length());
    }
    return result;
  }

}
