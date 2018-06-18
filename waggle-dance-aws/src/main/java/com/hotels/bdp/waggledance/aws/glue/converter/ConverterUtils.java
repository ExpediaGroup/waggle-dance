/**
 * Copyright (C) 2016-2018 Expedia Inc.
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
package com.hotels.bdp.waggledance.aws.glue.converter;

import com.amazonaws.services.glue.model.Table;

public class ConverterUtils {
  public static final String INDEX_DEFERRED_REBUILD = "DeferredRebuild";
  public static final String INDEX_TABLE_NAME = "IndexTableName";
  public static final String INDEX_HANDLER_CLASS = "IndexHandlerClass";
  public static final String INDEX_DB_NAME = "DbName";
  public static final String INDEX_ORIGIN_TABLE_NAME = "OriginTableName";

  public ConverterUtils() {}

  private static final com.google.gson.Gson gson = new com.google.gson.Gson();

  public static String catalogTableToString(Table table) {
    return gson.toJson(table);
  }

  public static Table stringToCatalogTable(String input) {
    return (Table)gson.fromJson(input, Table.class);
  }
}
