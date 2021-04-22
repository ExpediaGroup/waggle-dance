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
package com.hotels.bdp.waggledance.mapping.model;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.CAT_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.DB_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.parseDbName;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.prependNotNullCatToDbName;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.MetaException;

public class PrefixMapping extends MetaStoreMappingDecorator {

  public PrefixMapping(MetaStoreMapping metaStoreMapping) {
    super(metaStoreMapping);
  }

  @Override
  public String transformOutboundDatabaseName(String databaseName)  {
      if(hasCatalogName(databaseName)) {
        String[] catalogAndDatabase = new String[0];
        try {
          catalogAndDatabase = parseDbName(databaseName, null);
        }
        catch (MetaException e) {
          //FIXME
          throw new RuntimeException(e);
        }
        String dbName = catalogAndDatabase[DB_NAME];
        String catalogName = catalogAndDatabase[CAT_NAME];
        String prefixedDbName = getDatabasePrefix() + super.transformOutboundDatabaseName(dbName);
        return prependNotNullCatToDbName(catalogName, prefixedDbName);
      }else {
        return getDatabasePrefix() + super.transformOutboundDatabaseName(databaseName);
      }
  }

  private static boolean hasCatalogName(String dbName) {
    return dbName != null && dbName.length() > 0 &&
            dbName.charAt(0) == '@';
  }

  @Override
  public List<String> transformOutboundDatabaseNameMultiple(String databaseName) {
    List<String> outbound = super.transformOutboundDatabaseNameMultiple(databaseName);
    List<String> result = new ArrayList<>(outbound.size());
    for (String outboundDatabase : outbound) {
      result.add(transformOutboundDatabaseName(outboundDatabase));
    }
    return result;
  }

  @Override
  public String transformInboundDatabaseName(String databaseName) {
    if(hasCatalogName(databaseName)) {
      String[] catalogAndDatabase = new String[0];
      try {
        catalogAndDatabase = parseDbName(databaseName, null);
      }
      catch (MetaException e) {
        //FIXME
        throw new RuntimeException(e);
      }
      String dbName = catalogAndDatabase[DB_NAME];
      String catalogName = catalogAndDatabase[CAT_NAME];
      return prependNotNullCatToDbName(catalogName, internalTransformInboundDatabaseName(dbName));
    }
    return internalTransformInboundDatabaseName(databaseName);
  }

  private String internalTransformInboundDatabaseName(String databaseName) {
    String result = super.transformInboundDatabaseName(databaseName);
    if (result.startsWith(getDatabasePrefix())) {
      return result.substring(getDatabasePrefix().length());
    }
    return result;
  }

}
