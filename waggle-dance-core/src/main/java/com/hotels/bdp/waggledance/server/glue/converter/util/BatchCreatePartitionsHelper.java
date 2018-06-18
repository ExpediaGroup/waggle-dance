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
package com.hotels.bdp.waggledance.server.glue.converter.util;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchCreatePartitionRequest;
import com.amazonaws.services.glue.model.BatchCreatePartitionResult;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;
import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.server.glue.converter.CatalogToHiveConverter;
import com.hotels.bdp.waggledance.server.glue.converter.GlueInputConverter;

public final class BatchCreatePartitionsHelper {
  private static final Logger logger = Logger.getLogger(BatchCreatePartitionsHelper.class);

  private final AWSGlue client;
  private final String databaseName;
  private final String tableName;
  private final List<Partition> partitions;
  private final boolean ifNotExists;
  private Map<PartitionKey, Partition> partitionMap;
  private List<Partition> partitionsFailed;
  private TException firstTException;

  public BatchCreatePartitionsHelper(
      AWSGlue client,
      String databaseName,
      String tableName,
      List<Partition> partitions,
      boolean ifNotExists) {
    this.client = client;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.partitions = partitions;
    this.ifNotExists = ifNotExists;
  }

  public BatchCreatePartitionsHelper createPartitions() {
    partitionMap = PartitionUtils.buildPartitionMap(partitions);
    partitionsFailed = Lists.newArrayList();

    BatchCreatePartitionRequest request = new BatchCreatePartitionRequest()
        .withDatabaseName(databaseName)
        .withTableName(tableName)
        .withPartitionInputList(GlueInputConverter.convertToPartitionInputs(partitionMap.values()));
    try {
      BatchCreatePartitionResult result = client.batchCreatePartition(request);
      processResult(result);
    } catch (Exception e) {
      logger.error("Exception thrown while creating partitions in DataCatalog: ", e);
      firstTException = CatalogToHiveConverter.wrapInHiveException(e);
      if (PartitionUtils.isInvalidUserInputException(e)) {
        setAllFailed();
      } else {
        checkIfPartitionsCreated();
      }
    }
    return this;
  }

  private void setAllFailed() {
    partitionsFailed = partitions;
    partitionMap.clear();
  }

  private void processResult(BatchCreatePartitionResult result) {
    List<PartitionError> partitionErrors = result.getErrors();
    if ((partitionErrors == null) || (partitionErrors.isEmpty())) {
      return;
    }

    logger.error(String.format("BatchCreatePartitions failed to create %d out of %d partitions. \n",
        new Object[] { Integer.valueOf(partitionErrors.size()), Integer.valueOf(partitionMap.size()) }));

    for (PartitionError partitionError : partitionErrors) {
      Partition partitionFailed = (Partition) partitionMap
          .remove(new PartitionKey(partitionError.getPartitionValues()));

      TException exception = CatalogToHiveConverter.errorDetailToHiveException(partitionError.getErrorDetail());
      if ((!ifNotExists) || (!(exception instanceof AlreadyExistsException))) {

        logger.error(exception);
        if (firstTException == null) {
          firstTException = exception;
        }
        partitionsFailed.add(partitionFailed);
      }
    }
  }

  private void checkIfPartitionsCreated() {
    for (Partition partition : partitions) {
      if (!partitionExists(partition)) {
        partitionsFailed.add(partition);
        partitionMap.remove(new PartitionKey(partition));
      }
    }
  }

  private boolean partitionExists(Partition partition) {
    GetPartitionRequest request = new GetPartitionRequest()
        .withDatabaseName(partition.getDatabaseName())
        .withTableName(partition.getTableName())
        .withPartitionValues(partition.getValues());
    try {
      GetPartitionResult result = client.getPartition(request);
      Partition partitionReturned = result.getPartition();
      return partitionReturned != null;
    } catch (EntityNotFoundException e) {
      return false;
    } catch (Exception e) {
      logger.error(String.format("Get partition request %s failed. ", new Object[] { request.toString() }), e);
    }
    return false;
  }

  public TException getFirstTException() {
    return firstTException;
  }

  public Collection<Partition> getPartitionsCreated() {
    return partitionMap.values();
  }

  public List<Partition> getPartitionsFailed() {
    return partitionsFailed;
  }
}
