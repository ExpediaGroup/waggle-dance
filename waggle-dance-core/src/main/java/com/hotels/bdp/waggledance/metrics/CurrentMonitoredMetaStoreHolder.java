/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.waggledance.metrics;

import org.apache.commons.lang3.StringUtils;

public final class CurrentMonitoredMetaStoreHolder {

  private CurrentMonitoredMetaStoreHolder() {
  }

  private static final String ALL_METASTORES = "all";
  private static final ThreadLocal<String> MONITORED_METASTORE = new ThreadLocal<>();

  public static void monitorMetastore() {
    monitorMetastore(ALL_METASTORES);
  }

  public static void monitorMetastore(String metastoreName) {
    MONITORED_METASTORE.set(metastoreName);
  }

  public static String getMonitorMetastore() {
    String metastoreName = MONITORED_METASTORE.get();
    if (StringUtils.isNotBlank(metastoreName)) {
      return metastoreName;
    }
    return ALL_METASTORES;
  }

}
