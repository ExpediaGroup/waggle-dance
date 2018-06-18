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

import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.FunctionType;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.ResourceType;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.amazonaws.services.glue.model.Column;
import com.amazonaws.services.glue.model.ErrorDetail;
import com.amazonaws.services.glue.model.UserDefinedFunction;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class CatalogToHiveConverter {
  private static final Logger logger = Logger.getLogger(CatalogToHiveConverter.class);

  private static ImmutableMap<String, HiveException> EXCEPTION_MAP = ImmutableMap
      .<String, HiveException> builder()
      .put("AlreadyExistsException", new HiveException() {
        public TException get(String msg) {
          return new AlreadyExistsException(msg);
        }
      })

      .put("InvalidInputException", new HiveException() {

        public TException get(String msg) {

          return new InvalidObjectException(msg);
        }
      })

      .put("InternalServiceException", new HiveException() {

        public TException get(String msg) {

          return new MetaException(msg);
        }
      })

      .put("ResourceNumberLimitExceededException", new HiveException() {

        public TException get(String msg) {

          return new MetaException(msg);
        }
      })

      .put("OperationTimeoutException", new HiveException() {

        public TException get(String msg) {

          return new MetaException(msg);
        }
      })

      .put("EntityNotFoundException", new HiveException() {

        public TException get(String msg) {

          return new NoSuchObjectException(msg);
        }
      })
      .build();

  public CatalogToHiveConverter() {}

  public static TException wrapInHiveException(Exception e) {
    return getHiveException(e.getClass().getSimpleName(), e.getMessage());
  }

  public static TException errorDetailToHiveException(ErrorDetail errorDetail) {
    return getHiveException(errorDetail.getErrorCode(), errorDetail.getErrorMessage());
  }

  private static TException getHiveException(String errorName, String errorMsg) {
    if (EXCEPTION_MAP.containsKey(errorName)) {
      return ((HiveException) EXCEPTION_MAP.get(errorName)).get(errorMsg);
    }
    logger.warn("Hive Exception type not found for " + errorName);
    return new MetaException(errorMsg);
  }

  public static org.apache.hadoop.hive.metastore.api.Database convertDatabase(
      com.amazonaws.services.glue.model.Database catalogDatabase) {
    org.apache.hadoop.hive.metastore.api.Database hiveDatabase = new org.apache.hadoop.hive.metastore.api.Database();
    hiveDatabase.setName(catalogDatabase.getName());
    hiveDatabase.setDescription(catalogDatabase.getDescription());
    String location = catalogDatabase.getLocationUri();
    hiveDatabase.setLocationUri(location == null ? "" : location);
    hiveDatabase.setParameters(catalogDatabase.getParameters());
    return hiveDatabase;
  }

  public static FieldSchema convertFieldSchema(Column catalogFieldSchema) {
    FieldSchema hiveFieldSchema = new FieldSchema();
    hiveFieldSchema.setType(catalogFieldSchema.getType());
    hiveFieldSchema.setName(catalogFieldSchema.getName());
    hiveFieldSchema.setComment(catalogFieldSchema.getComment());

    return hiveFieldSchema;
  }

  public static List<FieldSchema> convertFieldSchemaList(List<Column> catalogFieldSchemaList) {
    List<FieldSchema> hiveFieldSchemaList = new ArrayList();
    if (catalogFieldSchemaList == null) {
      return hiveFieldSchemaList;
    }
    for (Column catalogFieldSchema : catalogFieldSchemaList) {
      hiveFieldSchemaList.add(convertFieldSchema(catalogFieldSchema));
    }

    return hiveFieldSchemaList;
  }

  public static org.apache.hadoop.hive.metastore.api.Table convertTable(
      com.amazonaws.services.glue.model.Table catalogTable,
      String dbname) {
    org.apache.hadoop.hive.metastore.api.Table hiveTable = new org.apache.hadoop.hive.metastore.api.Table();
    hiveTable.setDbName(dbname);
    hiveTable.setTableName(catalogTable.getName());
    Date createTime = catalogTable.getCreateTime();
    hiveTable.setCreateTime(createTime == null ? 0 : (int) (createTime.getTime() / 1000L));
    hiveTable.setOwner(catalogTable.getOwner());
    Date lastAccessedTime = catalogTable.getLastAccessTime();
    hiveTable.setLastAccessTime(lastAccessedTime == null ? 0 : (int) (lastAccessedTime.getTime() / 1000L));
    hiveTable.setRetention(catalogTable.getRetention().intValue());
    hiveTable.setSd(convertStorageDescriptor(catalogTable.getStorageDescriptor()));
    hiveTable.setPartitionKeys(convertFieldSchemaList(catalogTable.getPartitionKeys()));

    Map<String, String> parameterMap = catalogTable.getParameters();
    if (parameterMap == null) {
      parameterMap = Maps.newHashMap();
    }
    hiveTable.setParameters(parameterMap);
    hiveTable.setViewOriginalText(catalogTable.getViewOriginalText());
    hiveTable.setViewExpandedText(catalogTable.getViewExpandedText());
    hiveTable.setTableType(catalogTable.getTableType());

    return hiveTable;
  }

  public static TableMeta convertTableMeta(com.amazonaws.services.glue.model.Table catalogTable, String dbName) {
    TableMeta tableMeta = new TableMeta();
    tableMeta.setDbName(dbName);
    tableMeta.setTableName(catalogTable.getName());
    tableMeta.setTableType(catalogTable.getTableType());
    if (catalogTable.getParameters().containsKey("comment")) {
      tableMeta.setComments((String) catalogTable.getParameters().get("comment"));
    }
    return tableMeta;
  }

  public static org.apache.hadoop.hive.metastore.api.StorageDescriptor convertStorageDescriptor(
      com.amazonaws.services.glue.model.StorageDescriptor catalogSd) {
    org.apache.hadoop.hive.metastore.api.StorageDescriptor hiveSd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor();
    hiveSd.setCols(convertFieldSchemaList(catalogSd.getColumns()));
    hiveSd.setLocation(catalogSd.getLocation());
    hiveSd.setInputFormat(catalogSd.getInputFormat());
    hiveSd.setOutputFormat(catalogSd.getOutputFormat());
    hiveSd.setCompressed(catalogSd.getCompressed().booleanValue());
    hiveSd.setNumBuckets(catalogSd.getNumberOfBuckets().intValue());
    hiveSd.setSerdeInfo(convertSerDeInfo(catalogSd.getSerdeInfo()));
    hiveSd.setBucketCols(
        (List) ObjectUtils.firstNonNull(new List[] { catalogSd.getBucketColumns(), Lists.newArrayList() }));
    hiveSd.setSortCols(convertOrderList(catalogSd.getSortColumns()));
    hiveSd.setParameters((Map) ObjectUtils.firstNonNull(new Map[] { catalogSd.getParameters(), Maps.newHashMap() }));
    hiveSd.setSkewedInfo(convertSkewedInfo(catalogSd.getSkewedInfo()));
    hiveSd.setStoredAsSubDirectories(catalogSd.getStoredAsSubDirectories().booleanValue());

    return hiveSd;
  }

  public static org.apache.hadoop.hive.metastore.api.Order convertOrder(
      com.amazonaws.services.glue.model.Order catalogOrder) {
    org.apache.hadoop.hive.metastore.api.Order hiveOrder = new org.apache.hadoop.hive.metastore.api.Order();
    hiveOrder.setCol(catalogOrder.getColumn());
    hiveOrder.setOrder(catalogOrder.getSortOrder().intValue());

    return hiveOrder;
  }

  public static List<org.apache.hadoop.hive.metastore.api.Order> convertOrderList(
      List<com.amazonaws.services.glue.model.Order> catalogOrderList) {
    List<org.apache.hadoop.hive.metastore.api.Order> hiveOrderList = new ArrayList();
    if (catalogOrderList == null) {
      return hiveOrderList;
    }
    for (com.amazonaws.services.glue.model.Order catalogOrder : catalogOrderList) {
      hiveOrderList.add(convertOrder(catalogOrder));
    }

    return hiveOrderList;
  }

  public static org.apache.hadoop.hive.metastore.api.SerDeInfo convertSerDeInfo(
      com.amazonaws.services.glue.model.SerDeInfo catalogSerDeInfo) {
    org.apache.hadoop.hive.metastore.api.SerDeInfo hiveSerDeInfo = new org.apache.hadoop.hive.metastore.api.SerDeInfo();
    hiveSerDeInfo.setName(catalogSerDeInfo.getName());
    hiveSerDeInfo.setParameters(catalogSerDeInfo.getParameters());
    hiveSerDeInfo.setSerializationLib(catalogSerDeInfo.getSerializationLibrary());

    return hiveSerDeInfo;
  }

  public static org.apache.hadoop.hive.metastore.api.SkewedInfo convertSkewedInfo(
      com.amazonaws.services.glue.model.SkewedInfo catalogSkewedInfo) {
    if (catalogSkewedInfo == null) {
      return null;
    }
    org.apache.hadoop.hive.metastore.api.SkewedInfo hiveSkewedInfo = new org.apache.hadoop.hive.metastore.api.SkewedInfo();
    hiveSkewedInfo.setSkewedColNames(catalogSkewedInfo.getSkewedColumnNames());
    hiveSkewedInfo.setSkewedColValues(convertSkewedValue(catalogSkewedInfo.getSkewedColumnValues()));
    hiveSkewedInfo
        .setSkewedColValueLocationMaps(convertSkewedMap(catalogSkewedInfo.getSkewedColumnValueLocationMaps()));
    return hiveSkewedInfo;
  }

  public static Index convertTableObjectToIndex(com.amazonaws.services.glue.model.Table catalogTable) {
    Index hiveIndex = new Index();
    Map<String, String> parameters = catalogTable.getParameters();
    hiveIndex.setIndexName(catalogTable.getName());
    hiveIndex.setCreateTime((int) (catalogTable.getCreateTime().getTime() / 1000L));
    hiveIndex.setLastAccessTime((int) (catalogTable.getLastAccessTime().getTime() / 1000L));
    hiveIndex.setSd(convertStorageDescriptor(catalogTable.getStorageDescriptor()));
    hiveIndex.setParameters(catalogTable.getParameters());

    hiveIndex.setDeferredRebuild(((String) parameters.get("DeferredRebuild")).equals("TRUE"));
    hiveIndex.setIndexHandlerClass((String) parameters.get("IndexHandlerClass"));
    hiveIndex.setDbName((String) parameters.get("DbName"));
    hiveIndex.setOrigTableName((String) parameters.get("OriginTableName"));
    hiveIndex.setIndexTableName((String) parameters.get("IndexTableName"));

    return hiveIndex;
  }

  public static org.apache.hadoop.hive.metastore.api.Partition convertPartition(
      com.amazonaws.services.glue.model.Partition src) {
    org.apache.hadoop.hive.metastore.api.Partition tgt = new org.apache.hadoop.hive.metastore.api.Partition();
    Date createTime = src.getCreationTime();
    if (createTime != null) {
      tgt.setCreateTime((int) (createTime.getTime() / 1000L));
      tgt.setCreateTimeIsSet(true);
    } else {
      tgt.setCreateTimeIsSet(false);
    }
    String dbName = src.getDatabaseName();
    if (dbName != null) {
      tgt.setDbName(dbName);
      tgt.setDbNameIsSet(true);
    } else {
      tgt.setDbNameIsSet(false);
    }
    Date lastAccessTime = src.getLastAccessTime();
    if (lastAccessTime != null) {
      tgt.setLastAccessTime((int) (lastAccessTime.getTime() / 1000L));
      tgt.setLastAccessTimeIsSet(true);
    } else {
      tgt.setLastAccessTimeIsSet(false);
    }
    Map<String, String> params = src.getParameters();

    if (params == null) {
      params = Maps.newHashMap();
    }

    tgt.setParameters(params);
    tgt.setParametersIsSet(true);

    String tableName = src.getTableName();
    if (tableName != null) {
      tgt.setTableName(tableName);
      tgt.setTableNameIsSet(true);
    } else {
      tgt.setTableNameIsSet(false);
    }

    List<String> values = src.getValues();
    if (values != null) {
      tgt.setValues(values);
      tgt.setValuesIsSet(true);
    } else {
      tgt.setValuesIsSet(false);
    }

    com.amazonaws.services.glue.model.StorageDescriptor sd = src.getStorageDescriptor();
    if (sd != null) {
      org.apache.hadoop.hive.metastore.api.StorageDescriptor hiveSd = convertStorageDescriptor(sd);
      tgt.setSd(hiveSd);
      tgt.setSdIsSet(true);
    } else {
      tgt.setSdIsSet(false);
    }

    return tgt;
  }

  public static List<org.apache.hadoop.hive.metastore.api.Partition> convertPartitions(
      List<com.amazonaws.services.glue.model.Partition> src) {
    if (src == null) {
      return null;
    }

    List<org.apache.hadoop.hive.metastore.api.Partition> target = Lists.newArrayList();
    for (com.amazonaws.services.glue.model.Partition partition : src) {
      target.add(convertPartition(partition));
    }
    return target;
  }

  public static List<String> convertStringToList(String s) {
    if (s == null) {
      return null;
    }
    List<String> listString = new ArrayList();
    for (int i = 0; i < s.length();) {
      StringBuilder length = new StringBuilder();
      for (int j = i; j < s.length(); j++) {
        if (s.charAt(j) != '$') {
          length.append(s.charAt(j));
        } else {
          int lengthOfString = Integer.valueOf(length.toString()).intValue();
          listString.add(s.substring(j + 1, j + 1 + lengthOfString));
          i = j + 1 + lengthOfString;
          break;
        }
      }
    }
    return listString;
  }

  public static Map<List<String>, String> convertSkewedMap(Map<String, String> coralSkewedMap) {
    if (coralSkewedMap == null) {
      return null;
    }
    Map<List<String>, String> coreSkewedMap = new HashMap();
    for (String coralKey : coralSkewedMap.keySet()) {
      coreSkewedMap.put(convertStringToList(coralKey), coralSkewedMap.get(coralKey));
    }
    return coreSkewedMap;
  }

  public static List<List<String>> convertSkewedValue(List<String> coralSkewedValue) {
    if (coralSkewedValue == null) {
      return null;
    }
    List<List<String>> coreSkewedValue = new ArrayList();
    for (int i = 0; i < coralSkewedValue.size(); i++) {
      coreSkewedValue.add(convertStringToList((String) coralSkewedValue.get(i)));
    }
    return coreSkewedValue;
  }

  public static org.apache.hadoop.hive.metastore.api.PrincipalType convertPrincipalType(
      com.amazonaws.services.glue.model.PrincipalType catalogPrincipalType) {
    if (catalogPrincipalType == null) {
      return null;
    }

    if (catalogPrincipalType == com.amazonaws.services.glue.model.PrincipalType.GROUP)
      return org.apache.hadoop.hive.metastore.api.PrincipalType.GROUP;
    if (catalogPrincipalType == com.amazonaws.services.glue.model.PrincipalType.USER)
      return org.apache.hadoop.hive.metastore.api.PrincipalType.USER;
    if (catalogPrincipalType == com.amazonaws.services.glue.model.PrincipalType.ROLE) {
      return org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
    }
    throw new RuntimeException("Unknown principal type:" + catalogPrincipalType.name());
  }

  public static Function convertFunction(String dbName, UserDefinedFunction catalogFunction) {
    if (catalogFunction == null) {
      return null;
    }
    Function hiveFunction = new Function();
    hiveFunction.setClassName(catalogFunction.getClassName());
    hiveFunction.setCreateTime((int) (catalogFunction.getCreateTime().getTime() / 1000L));
    hiveFunction.setDbName(dbName);
    hiveFunction.setFunctionName(catalogFunction.getFunctionName());
    hiveFunction.setFunctionType(FunctionType.JAVA);
    hiveFunction.setOwnerName(catalogFunction.getOwnerName());
    hiveFunction.setOwnerType(convertPrincipalType(
        com.amazonaws.services.glue.model.PrincipalType.fromValue(catalogFunction.getOwnerType())));
    hiveFunction.setResourceUris(convertResourceUriList(catalogFunction.getResourceUris()));
    return hiveFunction;
  }

  public static List<org.apache.hadoop.hive.metastore.api.ResourceUri> convertResourceUriList(
      List<com.amazonaws.services.glue.model.ResourceUri> catalogResourceUriList) {
    if (catalogResourceUriList == null) {
      return null;
    }
    List<org.apache.hadoop.hive.metastore.api.ResourceUri> hiveResourceUriList = new ArrayList();
    for (com.amazonaws.services.glue.model.ResourceUri catalogResourceUri : catalogResourceUriList) {
      org.apache.hadoop.hive.metastore.api.ResourceUri hiveResourceUri = new org.apache.hadoop.hive.metastore.api.ResourceUri();
      hiveResourceUri.setUri(catalogResourceUri.getUri());
      if (catalogResourceUri.getResourceType() != null) {
        hiveResourceUri.setResourceType(ResourceType.valueOf(catalogResourceUri.getResourceType()));
      }
      hiveResourceUriList.add(hiveResourceUri);
    }

    return hiveResourceUriList;
  }

  static abstract interface HiveException {
    public abstract TException get(String paramString);
  }
}
