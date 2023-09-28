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
package com.hotels.bdp.waggledance.server.security.ranger;

import static java.lang.String.format;

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

class Resource extends RangerAccessResourceImpl {
  private static final String KEY_DATABASE = "database";
  private static final String KEY_TABLE = "table";
  private static final String KEY_UDF = "udf";
  private static final String KEY_COLUMN = "column";

  private Type type = null;

  Resource(Type objectType, String database, String tableOrUdf) {
    this(objectType, database, tableOrUdf, null);
  }

  Resource(Type objectType, String database, String tableOrUdf, String column) {
    this.type = objectType;

    switch (objectType) {
      case DATABASE:
        setValue(KEY_DATABASE, database);
        break;

      case FUNCTION:
        setValue(KEY_DATABASE, database);
        setValue(KEY_UDF, tableOrUdf);
        break;

      case TABLE:
      case VIEW:
      case INDEX:
      case PARTITION:
        setValue(KEY_DATABASE, database);
        setValue(KEY_TABLE, tableOrUdf);
        break;

      case NONE:
      case URI:
      default:
        break;
    }
  }

  public Type getType() {
    return type;
  }

  public String getDatabase() {
    return (String) getValue(KEY_DATABASE);
  }

  public String getTable() {
    return (String) getValue(KEY_TABLE);
  }

  public String getUdf() {
    return (String) getValue(KEY_UDF);
  }

  public String getColumn() {
    return (String) getValue(KEY_COLUMN);
  }

  public String getLoggingMessage() {
    switch (type) {
      case DATABASE:
        return format("database `%s`", getDatabase());
      case FUNCTION:
        return format("function `%s.%s`", getDatabase(), getUdf());
      case TABLE:
      case VIEW:
      case INDEX:
      case PARTITION:
        return format("%s `%s.%s`", type.name().toLowerCase(), getDatabase(), getTable());
      case COLUMN:
        return format("column `%s` of `%s.%s`", getColumn(), getDatabase(), getTable());
      case NONE:
      case URI:
      default:
        throw new IllegalArgumentException(format("Unsupported resource type `%s`", type.name()));
    }
  }

  public enum Type {
    NONE, DATABASE, TABLE, VIEW, PARTITION, INDEX, COLUMN, FUNCTION, URI
  }

}
