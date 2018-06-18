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

import java.util.List;

import com.amazonaws.services.glue.model.Partition;

public class PartitionKey {
  private final List<String> partitionValues;
  private final int hashCode;

  public PartitionKey(Partition partition) {
    this(partition.getValues());
  }

  public PartitionKey(List<String> partitionValues) {
    if (partitionValues == null) {
      throw new IllegalArgumentException("Partition values cannot be null");
    }
    this.partitionValues = partitionValues;
    hashCode = partitionValues.hashCode();
  }

  public boolean equals(Object other) {
    return (this == other)
        || ((other != null) && ((other instanceof PartitionKey)) && (partitionValues.equals(partitionValues)));
  }

  public int hashCode() {
    return hashCode;
  }

  List<String> getValues() {
    return partitionValues;
  }
}
