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
package com.hotels.bdp.waggledance.server.glue.converter;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.Index;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.UserDefinedFunction;

public class HiveToCatalogConverter {
  public HiveToCatalogConverter() {}

  public static com.amazonaws.services.glue.model.Database convertDatabase(
      org.apache.hadoop.hive.metastore.api.Database hiveDatabase) {
    com.amazonaws.services.glue.model.Database catalogDatabase = new com.amazonaws.services.glue.model.Database();
    catalogDatabase.setName(hiveDatabase.getName());
    catalogDatabase.setDescription(hiveDatabase.getDescription());
    catalogDatabase.setLocationUri(hiveDatabase.getLocationUri());
    catalogDatabase.setParameters(hiveDatabase.getParameters());
    return catalogDatabase;
  }

  public static com.amazonaws.services.glue.model.Table convertTable(
      org.apache.hadoop.hive.metastore.api.Table hiveTable) {
    com.amazonaws.services.glue.model.Table catalogTable = new com.amazonaws.services.glue.model.Table();
    catalogTable.setRetention(Integer.valueOf(hiveTable.getRetention()));
    catalogTable.setPartitionKeys(convertFieldSchemaList(hiveTable.getPartitionKeys()));
    catalogTable.setTableType(hiveTable.getTableType());
    catalogTable.setName(hiveTable.getTableName());
    catalogTable.setOwner(hiveTable.getOwner());
    catalogTable.setCreateTime(new Date(hiveTable.getCreateTime() * 1000L));
    catalogTable.setLastAccessTime(new Date(hiveTable.getLastAccessTime() * 1000L));
    catalogTable.setStorageDescriptor(convertStorageDescriptor(hiveTable.getSd()));
    catalogTable.setParameters(hiveTable.getParameters());
    catalogTable.setViewExpandedText(hiveTable.getViewExpandedText());
    catalogTable.setViewOriginalText(hiveTable.getViewOriginalText());

    return catalogTable;
  }

  public static com.amazonaws.services.glue.model.StorageDescriptor convertStorageDescriptor(
      org.apache.hadoop.hive.metastore.api.StorageDescriptor hiveSd) {
    com.amazonaws.services.glue.model.StorageDescriptor catalogSd = new com.amazonaws.services.glue.model.StorageDescriptor();

    catalogSd.setNumberOfBuckets(Integer.valueOf(hiveSd.getNumBuckets()));
    catalogSd.setCompressed(Boolean.valueOf(hiveSd.isCompressed()));
    catalogSd.setParameters(hiveSd.getParameters());
    catalogSd.setBucketColumns(hiveSd.getBucketCols());
    catalogSd.setColumns(convertFieldSchemaList(hiveSd.getCols()));
    catalogSd.setInputFormat(hiveSd.getInputFormat());
    catalogSd.setLocation(hiveSd.getLocation());
    catalogSd.setOutputFormat(hiveSd.getOutputFormat());
    catalogSd.setSerdeInfo(convertSerDeInfo(hiveSd.getSerdeInfo()));
    catalogSd.setSkewedInfo(convertSkewedInfo(hiveSd.getSkewedInfo()));
    catalogSd.setSortColumns(convertOrderList(hiveSd.getSortCols()));
    catalogSd.setStoredAsSubDirectories(Boolean.valueOf(hiveSd.isStoredAsSubDirectories()));

    return catalogSd;
  }

  public static Column convertFieldSchema(FieldSchema hiveFieldSchema) {
    Column catalogFieldSchema = new Column();

    catalogFieldSchema.setComment(hiveFieldSchema.getComment());
    catalogFieldSchema.setName(hiveFieldSchema.getName());
    catalogFieldSchema.setType(hiveFieldSchema.getType());

    return catalogFieldSchema;
  }

  public static List<Column> convertFieldSchemaList(List<FieldSchema> hiveFieldSchemaList) {
    List<Column> catalogFieldSchemaList = new ArrayList();

    for (FieldSchema hiveFs : hiveFieldSchemaList) {
      catalogFieldSchemaList.add(convertFieldSchema(hiveFs));
    }

    return catalogFieldSchemaList;
  }

  public static com.amazonaws.services.glue.model.SerDeInfo convertSerDeInfo(
      org.apache.hadoop.hive.metastore.api.SerDeInfo hiveSerDeInfo) {
    com.amazonaws.services.glue.model.SerDeInfo catalogSerDeInfo = new com.amazonaws.services.glue.model.SerDeInfo();
    catalogSerDeInfo.setName(hiveSerDeInfo.getName());
    catalogSerDeInfo.setParameters(hiveSerDeInfo.getParameters());
    catalogSerDeInfo.setSerializationLibrary(hiveSerDeInfo.getSerializationLib());

    return catalogSerDeInfo;
  }

  public static com.amazonaws.services.glue.model.SkewedInfo convertSkewedInfo(
      org.apache.hadoop.hive.metastore.api.SkewedInfo hiveSkewedInfo) {
    if (hiveSkewedInfo == null) {
      return null;
    }

    com.amazonaws.services.glue.model.SkewedInfo catalogSkewedInfo = new com.amazonaws.services.glue.model.SkewedInfo()
        .withSkewedColumnNames(hiveSkewedInfo.getSkewedColNames())
        .withSkewedColumnValues(convertSkewedValue(hiveSkewedInfo.getSkewedColValues()))
        .withSkewedColumnValueLocationMaps(convertSkewedMap(hiveSkewedInfo.getSkewedColValueLocationMaps()));
    return catalogSkewedInfo;
  }

  public static com.amazonaws.services.glue.model.Order convertOrder(
      org.apache.hadoop.hive.metastore.api.Order hiveOrder) {
    com.amazonaws.services.glue.model.Order order = new com.amazonaws.services.glue.model.Order();
    order.setColumn(hiveOrder.getCol());
    order.setSortOrder(Integer.valueOf(hiveOrder.getOrder()));

    return order;
  }

  public static List<com.amazonaws.services.glue.model.Order> convertOrderList(
      List<org.apache.hadoop.hive.metastore.api.Order> hiveOrderList) {
    if (hiveOrderList == null) {
      return null;
    }
    List<com.amazonaws.services.glue.model.Order> catalogOrderList = new ArrayList();
    for (org.apache.hadoop.hive.metastore.api.Order hiveOrder : hiveOrderList) {
      catalogOrderList.add(convertOrder(hiveOrder));
    }

    return catalogOrderList;
  }

  public static com.amazonaws.services.glue.model.Table convertIndexToTableObject(Index hiveIndex) {
    com.amazonaws.services.glue.model.Table catalogIndexTableObject = new com.amazonaws.services.glue.model.Table();
    catalogIndexTableObject.setName(hiveIndex.getIndexName());
    catalogIndexTableObject.setCreateTime(new Date(hiveIndex.getCreateTime() * 1000L));
    catalogIndexTableObject.setLastAccessTime(new Date(hiveIndex.getLastAccessTime() * 1000L));
    catalogIndexTableObject.setStorageDescriptor(convertStorageDescriptor(hiveIndex.getSd()));
    catalogIndexTableObject.setParameters(hiveIndex.getParameters());

    catalogIndexTableObject.getParameters().put("DeferredRebuild", hiveIndex.isDeferredRebuild() ? "TRUE" : "FALSE");
    catalogIndexTableObject.getParameters().put("IndexTableName", hiveIndex.getIndexTableName());
    catalogIndexTableObject.getParameters().put("IndexHandlerClass", hiveIndex.getIndexHandlerClass());
    catalogIndexTableObject.getParameters().put("DbName", hiveIndex.getDbName());
    catalogIndexTableObject.getParameters().put("OriginTableName", hiveIndex.getOrigTableName());

    return catalogIndexTableObject;
  }

  public static com.amazonaws.services.glue.model.Partition convertPartition(
      org.apache.hadoop.hive.metastore.api.Partition src) {
    com.amazonaws.services.glue.model.Partition tgt = new com.amazonaws.services.glue.model.Partition();

    tgt.setDatabaseName(src.getDbName());
    tgt.setTableName(src.getTableName());
    tgt.setCreationTime(new Date(src.getCreateTime() * 1000L));
    tgt.setLastAccessTime(new Date(src.getLastAccessTime() * 1000L));
    tgt.setParameters(src.getParameters());
    tgt.setStorageDescriptor(convertStorageDescriptor(src.getSd()));
    tgt.setValues(src.getValues());

    return tgt;
  }

  public static String convertListToString(List<String> list) {
    if (list == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < list.size(); i++) {
      String currentString = (String) list.get(i);
      sb.append(currentString.length() + "$" + currentString);
    }

    return sb.toString();
  }

  public static Map<String, String> convertSkewedMap(Map<List<String>, String> coreSkewedMap) {
    if (coreSkewedMap == null) {
      return null;
    }
    Map<String, String> catalogSkewedMap = new HashMap();
    for (List<String> coreKey : coreSkewedMap.keySet()) {
      catalogSkewedMap.put(convertListToString(coreKey), coreSkewedMap.get(coreKey));
    }
    return catalogSkewedMap;
  }

  public static List<String> convertSkewedValue(List<List<String>> coreSkewedValue) {
    if (coreSkewedValue == null) {
      return null;
    }
    List<String> catalogSkewedValue = new ArrayList();
    for (int i = 0; i < coreSkewedValue.size(); i++) {
      catalogSkewedValue.add(convertListToString((List) coreSkewedValue.get(i)));
    }

    return catalogSkewedValue;
  }

  public static UserDefinedFunction convertFunction(Function hiveFunction) {
    if (hiveFunction == null) {
      return null;
    }
    UserDefinedFunction catalogFunction = new UserDefinedFunction();
    catalogFunction.setClassName(hiveFunction.getClassName());
    catalogFunction.setFunctionName(hiveFunction.getFunctionName());
    catalogFunction.setCreateTime(new Date(hiveFunction.getCreateTime() * 1000L));
    catalogFunction.setOwnerName(hiveFunction.getOwnerName());
    if (hiveFunction.getOwnerType() != null) {
      catalogFunction.setOwnerType(hiveFunction.getOwnerType().name());
    }
    catalogFunction.setResourceUris(covertResourceUriList(hiveFunction.getResourceUris()));
    return catalogFunction;
  }

  public static List<com.amazonaws.services.glue.model.ResourceUri> covertResourceUriList(
      List<org.apache.hadoop.hive.metastore.api.ResourceUri> hiveResourceUriList) {
    if (hiveResourceUriList == null) {
      return null;
    }
    List<com.amazonaws.services.glue.model.ResourceUri> catalogResourceUriList = new ArrayList();
    for (org.apache.hadoop.hive.metastore.api.ResourceUri hiveResourceUri : hiveResourceUriList) {
      com.amazonaws.services.glue.model.ResourceUri catalogResourceUri = new com.amazonaws.services.glue.model.ResourceUri();
      catalogResourceUri.setUri(hiveResourceUri.getUri());
      if (hiveResourceUri.getResourceType() != null) {
        catalogResourceUri.setResourceType(hiveResourceUri.getResourceType().name());
      }
      catalogResourceUriList.add(catalogResourceUri);
    }
    return catalogResourceUriList;
  }
}
