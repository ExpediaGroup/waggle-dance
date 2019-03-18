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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.thrift.TException;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;

public class GetTableMetaRequest implements RequestCallable<List<TableMeta>> {

  private final DatabaseMapping mapping;
  private final String dbPattern;
  private final String tablePattern;
  private final List<String> tableTypes;
  private final BiFunction<TableMeta, DatabaseMapping, Boolean> filter;

  public GetTableMetaRequest(
      DatabaseMapping mapping,
      String dbPattern,
      String tablePattern,
      List<String> tableTypes,
      BiFunction<TableMeta, DatabaseMapping, Boolean> filter) {
    this.mapping = mapping;
    this.dbPattern = dbPattern;
    this.tablePattern = tablePattern;
    this.tableTypes = tableTypes;
    this.filter = filter;
  }

  @Override
  public List<TableMeta> call() throws TException {
    List<TableMeta> tables = mapping.getClient().get_table_meta(dbPattern, tablePattern, tableTypes);
    List<TableMeta> mappedTableMeta = new ArrayList<>();
    for (TableMeta tableMeta : tables) {
      if (filter.apply(tableMeta, mapping)) {
        mappedTableMeta.add(mapping.transformOutboundTableMeta(tableMeta));
      }
    }
    return mappedTableMeta;
  }

  @Override
  public DatabaseMapping getMapping() {
    return mapping;
  }
}
