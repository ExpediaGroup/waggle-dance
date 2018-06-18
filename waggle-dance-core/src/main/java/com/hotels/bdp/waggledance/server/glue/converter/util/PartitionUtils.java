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

import java.util.List;
import java.util.Map;

import com.amazonaws.services.glue.model.Partition;
import com.amazonaws.services.glue.model.PartitionValueList;
import com.google.common.collect.Maps;

public final class PartitionUtils {
  public PartitionUtils() {}

  public static Map<PartitionKey, Partition> buildPartitionMap(List<Partition> partitions) {
    Map<PartitionKey, Partition> partitionValuesMap = Maps.newHashMap();
    for (Partition partition : partitions) {
      partitionValuesMap.put(new PartitionKey(partition), partition);
    }
    return partitionValuesMap;
  }

  public static List<PartitionValueList> getPartitionValuesList(Map<PartitionKey, Partition> partitionMap) {
    List<PartitionValueList> partitionValuesList = com.google.common.collect.Lists.newArrayList();
    for (Map.Entry<PartitionKey, Partition> entry : partitionMap.entrySet()) {
      partitionValuesList.add(new PartitionValueList().withValues(((Partition) entry.getValue()).getValues()));
    }
    return partitionValuesList;
  }

  public static boolean isInvalidUserInputException(Exception e) {
    return ((e instanceof com.amazonaws.services.glue.model.EntityNotFoundException))
        || ((e instanceof com.amazonaws.services.glue.model.InvalidInputException));
  }
}
