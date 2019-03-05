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

import static com.hotels.bdp.waggledance.mapping.service.DatabaseMappingUtils.isWhitelisted;
import static com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler.PREFIXED_RESOLUTION_TYPE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.thrift.TException;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.util.Whitelist;

public class GetTableMetaRequest implements Callable<List<?>> {

  private final DatabaseMapping mapping;
  private final String dbPattern;
  private final String tablePattern;
  private final List<String> tableTypes;
  private final Map<String, Whitelist> mappedDbByPrefix;
  private final Map<String, DatabaseMapping> mappingsByDatabaseName;
  private final DatabaseMapping primaryMapping;
  private final String resolutionType;

  public GetTableMetaRequest(
      DatabaseMapping mapping,
      String dbPattern,
      String tablePattern,
      List<String> tableTypes,
      Map<String, Whitelist> mappedDbByPrefix,
      Map<String, DatabaseMapping> mappingsByDatabaseName,
      DatabaseMapping primaryMapping,
      String resolutionType) {
    this.mapping = mapping;
    this.dbPattern = dbPattern;
    this.tablePattern = tablePattern;
    this.tableTypes = tableTypes;
    this.mappedDbByPrefix = mappedDbByPrefix;
    this.mappingsByDatabaseName = mappingsByDatabaseName;
    this.primaryMapping = primaryMapping;
    this.resolutionType = resolutionType;
  }

  @Override
  public List<?> call() throws TException {
    List<TableMeta> tables = mapping.getClient().get_table_meta(dbPattern, tablePattern, tableTypes);
    List<TableMeta> mappedTableMeta = new ArrayList<>();
    for (TableMeta tableMeta : tables) {
      boolean shouldBeAdded = getConditionForResolutionType(tableMeta);
      if (shouldBeAdded) {
        mappedTableMeta.add(mapping.transformOutboundTableMeta(tableMeta));
      }
    }
    return mappedTableMeta;
  }

  private boolean getConditionForResolutionType(TableMeta tableMeta) {
    if (resolutionType.equals(PREFIXED_RESOLUTION_TYPE)) {
      return isWhitelisted(mapping.getDatabasePrefix(), tableMeta.getDbName(),
          mappedDbByPrefix);
    } else {
      boolean isPrimary = mapping.equals(primaryMapping);
      boolean isMapped = mappingsByDatabaseName.keySet().contains(tableMeta.getDbName());
      return isPrimary || isMapped;
    }
  }
}