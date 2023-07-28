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
package com.hotels.bdp.waggledance;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class TestUtils {

  public static final List<FieldSchema> DATA_COLUMNS = Arrays.asList(new FieldSchema("id", "bigint", ""),
      new FieldSchema("name", "string", ""), new FieldSchema("city", "tinyint", ""));

  public static final List<FieldSchema> PARTITION_COLUMNS = Arrays.asList(new FieldSchema("continent", "string", ""),
      new FieldSchema("country", "string", ""));

  static Table createUnpartitionedTable(
      HiveMetaStoreClient metaStoreClient,
      String database,
      String table,
      File location)
    throws TException {
    Table hiveTable = new Table();
    hiveTable.setDbName(database);
    hiveTable.setTableName(table);
    hiveTable.setTableType(TableType.EXTERNAL_TABLE.name());
    hiveTable.putToParameters("EXTERNAL", "TRUE");

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(DATA_COLUMNS);
    sd.setLocation(location.toURI().toString());
    sd.setParameters(new HashMap<>());
    sd.setSerdeInfo(new SerDeInfo());

    hiveTable.setSd(sd);

    metaStoreClient.createTable(hiveTable);

    return hiveTable;
  }

  static Table createPartitionedTable(HiveMetaStoreClient metaStoreClient, String database, String table, File location)
    throws Exception {

    Table hiveTable = new Table();
    hiveTable.setDbName(database);
    hiveTable.setTableName(table);
    hiveTable.setTableType(TableType.EXTERNAL_TABLE.name());
    hiveTable.putToParameters("EXTERNAL", "TRUE");

    hiveTable.setPartitionKeys(PARTITION_COLUMNS);

    StorageDescriptor sd = new StorageDescriptor();
    sd.setCols(DATA_COLUMNS);
    sd.setLocation(location.toURI().toString());
    sd.setParameters(new HashMap<>());
    sd.setSerdeInfo(new SerDeInfo());

    hiveTable.setSd(sd);

    metaStoreClient.createTable(hiveTable);

    return hiveTable;
  }

  static Partition newPartition(Table hiveTable, List<String> values, File location) {
    Partition partition = new Partition();
    partition.setDbName(hiveTable.getDbName());
    partition.setTableName(hiveTable.getTableName());
    partition.setValues(values);
    partition.setSd(new StorageDescriptor(hiveTable.getSd()));
    partition.getSd().setLocation(location.toURI().toString());
    return partition;
  }
}
