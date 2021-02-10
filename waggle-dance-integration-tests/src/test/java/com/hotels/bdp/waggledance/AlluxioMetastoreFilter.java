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
package com.hotels.bdp.waggledance;

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
public class AlluxioMetastoreFilter implements MetaStoreFilterHook {

  public static final String ALLUXIO_PREFIX = "alluxio://";

  public AlluxioMetastoreFilter(HiveConf conf) {
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
    setAlluxioLocation(table);
    return table;
  }

  @Override
  public List<Table> filterTables(List<Table> tableList) throws MetaException {
    for (Table table: tableList){
      setAlluxioLocation(table);
    }
    return tableList;
  }

  @Override
  public List<Partition> filterPartitions(List<Partition> partitionList) throws MetaException {
    for (Partition partition: partitionList){
      setAlluxioLocation(partition.getSd());
    }
    return partitionList;
  }

  @Override
  public List<PartitionSpec> filterPartitionSpecs(List<PartitionSpec> partitionSpecList) throws MetaException {
    for (PartitionSpec partitionSpec : partitionSpecList) {
      setAlluxioLocation(partitionSpec.getSharedSDPartitionSpec().getSd());
      filterPartitions(partitionSpec.getPartitionList().getPartitions());
    }
    return partitionSpecList;
  }

  @Override
  public Partition filterPartition(Partition partition) throws MetaException, NoSuchObjectException {
    setAlluxioLocation(partition);
    return partition;
  }

  @Override
  public List<String> filterPartitionNames(String dbName, String tblName,
      List<String> partitionNames) throws MetaException {
    return partitionNames;
  }

  @Override
  public Index filterIndex(Index index) throws MetaException, NoSuchObjectException {
    setAlluxioLocation(index.getSd());
    return index;
  }

  @Override
  public List<String> filterIndexNames(String dbName, String tblName, List<String> indexList) throws MetaException {
    return indexList;
  }

  @Override
  public List<Index> filterIndexes(List<Index> indexeList) throws MetaException {
    for (Index index: indexeList) {
      setAlluxioLocation(index.getSd());
    }
    return indexeList;
  }

  private void setAlluxioLocation(Table table) {
    setAlluxioLocation(table.getSd());
  }

  private void setAlluxioLocation(Partition partition) {
    setAlluxioLocation(partition.getSd());
  }

  private void setAlluxioLocation(StorageDescriptor sd) {
    String location = sd.getLocation();
    sd.setLocation(ALLUXIO_PREFIX + location);
  }

}
