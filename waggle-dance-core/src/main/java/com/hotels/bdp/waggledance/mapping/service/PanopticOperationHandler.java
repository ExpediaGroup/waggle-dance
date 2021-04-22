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
package com.hotels.bdp.waggledance.mapping.service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.service.requests.GetAllDatabasesByPatternRequest;
import com.hotels.bdp.waggledance.mapping.service.requests.GetAllFunctionsRequest;
import com.hotels.bdp.waggledance.mapping.service.requests.GetTableMetaRequest;
import com.hotels.bdp.waggledance.mapping.service.requests.SetUgiRequest;

/**
 * Class responsible for handling the Hive operations that need to combine results from multiple Hive Metastores
 */
public abstract class PanopticOperationHandler {

  protected static final long GET_DATABASES_TIMEOUT = TimeUnit.MILLISECONDS.toMillis(8000L);
  private static final long SET_UGI_TIMEOUT = TimeUnit.MILLISECONDS.toMillis(8000L);
  private static final long GET_TABLE_META_TIMEOUT = TimeUnit.MILLISECONDS.toMillis(400L);
  private static final long GET_ALL_FUNCTIONS_TIMEOUT = TimeUnit.SECONDS.toMillis(1L);

  protected abstract PanopticOperationExecutor getPanopticOperationExecutor();

  /**
   * Implements {@link HMSHandler#get_all_databases()} over multiple metastores
   *
   * @return list of all databases
   */
  public abstract List<String> getAllDatabases();

  /**
   * Implements {@link HMSHandler#get_database(String)} over multiple metastores
   *
   * @param databasePattern pattern to match
   * @return list of all databases that match the passed pattern
   */
  public abstract List<String> getAllDatabases(String databasePattern)
          throws MetaException;

  protected List<String> getAllDatabases(
      Map<DatabaseMapping, String> databaseMappingsForPattern,
      BiFunction<String, DatabaseMapping, Boolean> filter) {
    List<GetAllDatabasesByPatternRequest> allRequests = new ArrayList<>();

    for (Entry<DatabaseMapping, String> mappingWithPattern : databaseMappingsForPattern.entrySet()) {
      DatabaseMapping mapping = mappingWithPattern.getKey();
      GetAllDatabasesByPatternRequest databasesByPatternRequest = new GetAllDatabasesByPatternRequest(mapping,
              createInboundPattern(mappingWithPattern, mapping), filter);
      allRequests.add(databasesByPatternRequest);
    }
    List<String> result = getPanopticOperationExecutor()
        .executeRequests(allRequests, GET_DATABASES_TIMEOUT, "Can't fetch databases by pattern: {}");
    return result;
  }

  private String createInboundPattern(Entry<DatabaseMapping, String> mappingWithPattern, DatabaseMapping mapping)
  {
    String pattern = mappingWithPattern.getValue();
    if(pattern.startsWith("@")) {
      String sanifiedPattern = mappingWithPattern.getValue().equals("*") ? null : mappingWithPattern.getValue();
      pattern = MetaStoreUtils.prependCatalogToDbName(mapping.getCatalog(), pattern,null);
    }
    return pattern;
  }

  /**
   * Implements {@link HMSHandler#get_table_meta(String, String, List)} over multiple metastores
   *
   * @param databasePatterns database patterns to match
   * @param tablePatterns table patterns to match
   * @param tableTypes table types to match
   * @return list of table metadata
   */
  abstract public List<TableMeta> getTableMeta(String databasePatterns, String tablePatterns, List<String> tableTypes)
          throws MetaException;

  protected List<TableMeta> getTableMeta(
      String tablePatterns,
      List<String> tableTypes,
      Map<DatabaseMapping, String> databaseMappingsForPattern,
      BiFunction<TableMeta, DatabaseMapping, Boolean> filter) {
    List<GetTableMetaRequest> allRequests = new ArrayList<>();

    for (Entry<DatabaseMapping, String> mappingWithPattern : databaseMappingsForPattern.entrySet()) {
      DatabaseMapping mapping = mappingWithPattern.getKey();
      GetTableMetaRequest tableMetaRequest = new GetTableMetaRequest(mapping, createInboundPattern(mappingWithPattern, mapping),
          tablePatterns, tableTypes, filter);
      allRequests.add(tableMetaRequest);
    }

    List<TableMeta> result = getPanopticOperationExecutor()
        .executeRequests(allRequests, GET_TABLE_META_TIMEOUT, "Got exception fetching get_table_meta: {}");
    return result;
  }

  /**
   * Implements {@link HMSHandler#set_ugi(String, List)} over multiple metastores
   *
   * @param user_name user name
   * @param group_names group names
   * @return list
   */
  public List<String> setUgi(String user_name, List<String> group_names, List<DatabaseMapping> databaseMappings) {
    // set_ugi returns the user_name that was set (on EMR at least) we just combine them all to avoid duplicates.
    // Not sure if anything uses these results. We're assuming the order doesn't matter.
    List<SetUgiRequest> allRequests = new ArrayList<>();

    for (DatabaseMapping mapping : databaseMappings) {
      SetUgiRequest setUgiRequest = new SetUgiRequest(mapping, user_name, group_names);
      allRequests.add(setUgiRequest);
    }

    List<String> resultWithDuplicates = getPanopticOperationExecutor()
        .executeRequests(allRequests, SET_UGI_TIMEOUT, "Got exception setting UGI: {}");
    Set<String> result = new LinkedHashSet<>(resultWithDuplicates);
    return new ArrayList<>(result);
  }

  /**
   * Implements {@link HMSHandler#get_all_functions()} over multiple metastores
   *
   * @return GetAllFunctionsResponse (db's from federated metastores will be prefixed if necessary)
   */
  public GetAllFunctionsResponse getAllFunctions(List<DatabaseMapping> databaseMappings) {
    List<GetAllFunctionsRequest> allRequests = new ArrayList<>();

    for (DatabaseMapping mapping : databaseMappings) {
      GetAllFunctionsRequest getAllFunctionsRequest = new GetAllFunctionsRequest(mapping);
      allRequests.add(getAllFunctionsRequest);
    }

    List<GetAllFunctionsResponse> responses = getPanopticOperationExecutor()
        .executeRequests(allRequests, GET_ALL_FUNCTIONS_TIMEOUT, "Got exception fetching get_all_functions: {}");
    if (responses.isEmpty()) {
      return new GetAllFunctionsResponse();
    }
    GetAllFunctionsResponse result = new GetAllFunctionsResponse(responses.get(0));
    responses
        .stream()
        .skip(1)
        .filter(GetAllFunctionsResponse::isSetFunctions)
        .flatMap(response -> response.getFunctions().stream())
        .forEach(result::addToFunctions);

    return result;
  }
}
