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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.BatchDeletePartitionRequest;
import com.amazonaws.services.glue.model.BatchDeletePartitionResult;
import com.amazonaws.services.glue.model.EntityNotFoundException;
import com.amazonaws.services.glue.model.ErrorDetail;
import com.amazonaws.services.glue.model.GetPartitionRequest;
import com.amazonaws.services.glue.model.GetPartitionResult;
import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionError;

import com.hotels.bdp.waggledance.aws.glue.converter.CatalogToHiveConverter;

public final class BatchDeletePartitionsHelper {
  private static final Logger logger = Logger.getLogger(BatchDeletePartitionsHelper.class);
  private final AWSGlue client;
  private final String namespaceName;
  private final String tableName;
  private final List<Partition> partitions;
  private Map<PartitionKey, Partition> partitionMap;
  private TException firstTException;

  public BatchDeletePartitionsHelper(
      AWSGlue client,
      String namespaceName,
      String tableName,
      List<Partition> partitions) {
    this.client = client;
    this.namespaceName = namespaceName;
    this.tableName = tableName;
    this.partitions = partitions;
  }

  public BatchDeletePartitionsHelper deletePartitions() {
    this.partitionMap = PartitionUtils.buildPartitionMap(this.partitions);

    BatchDeletePartitionRequest request = new BatchDeletePartitionRequest()
        .withDatabaseName(this.namespaceName)
        .withTableName(this.tableName)
        .withPartitionsToDelete(PartitionUtils.getPartitionValuesList(this.partitionMap));
    try {
      BatchDeletePartitionResult result = this.client.batchDeletePartition(request);
      processResult(result);
    } catch (Exception e) {
      logger.error("Exception thrown while deleting partitions in DataCatalog: ", e);
      this.firstTException = CatalogToHiveConverter.wrapInHiveException(e);
      if (PartitionUtils.isInvalidUserInputException(e)) {
        setAllFailed();
      } else {
        checkIfPartitionsDeleted();
      }
    }
    return this;
  }

  private void setAllFailed() {
    this.partitionMap.clear();
  }

  private void processResult(BatchDeletePartitionResult batchDeletePartitionsResult) {
    List<PartitionError> partitionErrors = batchDeletePartitionsResult.getErrors();
    if ((partitionErrors == null) || (partitionErrors.isEmpty())) {
      return;
    }
    logger.error(String.format("BatchDeletePartitions failed to delete %d out of %d partitions. \n",
        new Object[] { Integer.valueOf(partitionErrors.size()), Integer.valueOf(this.partitionMap.size()) }));
    for (PartitionError partitionError : partitionErrors) {
      this.partitionMap.remove(new PartitionKey(partitionError.getPartitionValues()));
      ErrorDetail errorDetail = partitionError.getErrorDetail();
      logger.error(errorDetail.toString());
      if (this.firstTException == null) {
        this.firstTException = CatalogToHiveConverter.errorDetailToHiveException(errorDetail);
      }
    }
  }

  private void checkIfPartitionsDeleted() {
    for (Partition partition : this.partitions) {
      if (!partitionDeleted(partition)) {
        this.partitionMap.remove(new PartitionKey(partition));
      }
    }
  }

  private boolean partitionDeleted(Partition partition) {
    GetPartitionRequest request = new GetPartitionRequest()
        .withDatabaseName(partition.getDatabaseName())
        .withTableName(partition.getTableName())
        .withPartitionValues(partition.getValues());
    try {
      GetPartitionResult result = this.client.getPartition(request);
      Partition partitionReturned = result.getPartition();
      return partitionReturned == null;
    } catch (EntityNotFoundException e) {
      return true;
    } catch (Exception e) {
      logger.error(String.format("Get partition request %s failed. ", new Object[] { request.toString() }), e);
    }
    return false;
  }

  public TException getFirstTException() {
    return this.firstTException;
  }

  public Collection<Partition> getPartitionsDeleted() {
    return this.partitionMap.values();
  }
}
