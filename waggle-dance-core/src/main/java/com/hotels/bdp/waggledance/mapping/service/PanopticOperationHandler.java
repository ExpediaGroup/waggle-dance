/**
 * Copyright (C) 2016-2017 Expedia Inc.
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
package com.hotels.bdp.waggledance.mapping.service;

import java.util.List;

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.TableMeta;

/**
 * Class responsible for handling the Hive operations that need to combine results from multiple Hive Metastores
 */
public interface PanopticOperationHandler {

  /**
   * Implements {@link HMSHandler#get_all_databases()} over multiple metastores
   */
  List<String> getAllDatabases();

  /**
   * Implements {@link HMSHandler#get_database(String)} over multiple metastores
   */
  List<String> getAllDatabases(String databasePattern);

  /**
   * Implements {@link HMSHandler#get_table_meta(String, String, List)} over multiple metastores
   */
  List<TableMeta> getTableMeta(String databasePatterns, String tablePatterns, List<String> tableTypes);

  /**
   * Implements {@link HMSHandler#set_ugi(String, List)} over multiple metastores
   */
  List<String> setUgi(String user_name, List<String> group_names);

}
