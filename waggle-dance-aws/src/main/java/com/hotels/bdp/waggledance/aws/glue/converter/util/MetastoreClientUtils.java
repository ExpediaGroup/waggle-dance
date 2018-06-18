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
package com.hotels.bdp.waggledance.aws.glue.converter.util;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.hotels.bdp.waggledance.aws.glue.shims.AwsGlueHiveShims;
import com.hotels.bdp.waggledance.aws.glue.shims.ShimsLoader;

public final class MetastoreClientUtils {
  private static final AwsGlueHiveShims hiveShims = ShimsLoader.getHiveShims();

  private MetastoreClientUtils() {}

  public static boolean makeDirs(Warehouse wh, Path path) throws MetaException {
    Preconditions.checkNotNull(wh, "Warehouse cannot be null");
    Preconditions.checkNotNull(path, "Path cannot be null");

    boolean madeDir = false;
    if (!wh.isDir(path)) {
      if (!wh.mkdirs(path, true)) {
        throw new MetaException("Unable to create path: " + path);
      }
      madeDir = true;
    }
    return madeDir;
  }

  public static void validateTableObject(Table table, Configuration conf) throws InvalidObjectException {
    Preconditions.checkNotNull(table, "table cannot be null");
    Preconditions.checkNotNull(table.getSd(), "Table#StorageDescriptor cannot be null");

    if (!hiveShims.validateTableName(table.getTableName(), conf)) {
      throw new InvalidObjectException(table.getTableName() + " is not a valid object name");
    }
    String validate = MetaStoreUtils.validateTblColumns(table.getSd().getCols());
    if (validate != null) {
      throw new InvalidObjectException("Invalid column " + validate);
    }

    if (table.getPartitionKeys() != null) {
      validate = MetaStoreUtils.validateTblColumns(table.getPartitionKeys());
      if (validate != null) {
        throw new InvalidObjectException("Invalid partition column " + validate);
      }
    }
  }

  public static <K, V> Map<K, V> deepCopyMap(Map<K, V> originalMap) {
    Map<K, V> deepCopy = Maps.newHashMap();
    if (originalMap == null) {
      return deepCopy;
    }

    for (Map.Entry<K, V> entry : originalMap.entrySet()) {
      deepCopy.put(entry.getKey(), entry.getValue());
    }
    return deepCopy;
  }

  public static boolean isExternalTable(Table table) {
    if (table == null) {
      return false;
    }

    Map<String, String> params = table.getParameters();
    String paramsExternalStr = params == null ? null : (String) params.get("EXTERNAL");
    if (paramsExternalStr != null) {
      return "TRUE".equalsIgnoreCase(paramsExternalStr);
    }

    return (table.getTableType() != null) && (TableType.EXTERNAL_TABLE.name().equalsIgnoreCase(table.getTableType()));
  }
}
