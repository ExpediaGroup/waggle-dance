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

import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * For testing purposes
 * */
public class PrefixingMetastoreFilter implements MetaStoreFilterHook {

  public static final String PREFIX = "prefix-";

  public PrefixingMetastoreFilter(HiveConf conf) {
  }

  @Override
  public List<String> filterDatabases(List<String> dbList) throws MetaException {
    return dbList;
  }

  @Override
  public Database filterDatabase(Database dataBase) throws MetaException, NoSuchObjectException {
    return dataBase;
  }

  @Override
  public List<String> filterTableNames(String dbName, List<String> tableList) throws MetaException {
    return tableList;
  }

  @Override
  public Table filterTable(Table table) throws MetaException, NoSuchObjectException {
    setLocationPrefix(table);
    return table;
  }

  @Override
  public List<Table> filterTables(List<Table> tableList) throws MetaException {
    for (Table table: tableList){
      setLocationPrefix(table);
    }
    return tableList;
  }

  @Override
  public List<Partition> filterPartitions(List<Partition> partitionList) throws MetaException {
    for (Partition partition: partitionList){
      setLocationPrefix(partition.getSd());
    }
    return partitionList;
  }

  @Override
  public List<PartitionSpec> filterPartitionSpecs(List<PartitionSpec> partitionSpecList) throws MetaException {
    for (PartitionSpec partitionSpec : partitionSpecList) {
      setLocationPrefix(partitionSpec.getSharedSDPartitionSpec().getSd());
      filterPartitions(partitionSpec.getPartitionList().getPartitions());
    }
    return partitionSpecList;
  }

  @Override
  public Partition filterPartition(Partition partition) throws MetaException, NoSuchObjectException {
    setLocationPrefix(partition);
    return partition;
  }

  @Override
  public List<String> filterPartitionNames(String dbName, String tblName,
      List<String> partitionNames) throws MetaException {
    return partitionNames;
  }

  @Override
  public Index filterIndex(Index index) throws MetaException, NoSuchObjectException {
    setLocationPrefix(index.getSd());
    return index;
  }

  @Override
  public List<String> filterIndexNames(String dbName, String tblName, List<String> indexList) throws MetaException {
    return indexList;
  }

  @Override
  public List<Index> filterIndexes(List<Index> indexeList) throws MetaException {
    for (Index index: indexeList) {
      setLocationPrefix(index.getSd());
    }
    return indexeList;
  }

  private void setLocationPrefix(Table table) {
    setLocationPrefix(table.getSd());
  }

  private void setLocationPrefix(Partition partition) {
    setLocationPrefix(partition.getSd());
  }

  private void setLocationPrefix(StorageDescriptor sd) {
    String location = sd.getLocation();
    sd.setLocation(PREFIX + location);
  }

}
