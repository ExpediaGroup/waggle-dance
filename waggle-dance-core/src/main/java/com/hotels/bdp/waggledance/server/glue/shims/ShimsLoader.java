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
package com.hotels.bdp.waggledance.server.glue.shims;

import com.google.common.annotations.VisibleForTesting;

public final class ShimsLoader {
  private static AwsGlueHiveShims hiveShims;

  public ShimsLoader() {}

  public static synchronized AwsGlueHiveShims getHiveShims() {
    if (hiveShims == null) {
      hiveShims = loadHiveShims();
    }
    return hiveShims;
  }

  private static AwsGlueHiveShims loadHiveShims() {
    String hiveVersion = org.apache.hive.common.util.HiveVersionInfo.getShortVersion();
    if (AwsGlueHive2Shims.supportsVersion(hiveVersion)) {
      try {
        return (AwsGlueHiveShims) AwsGlueHive2Shims.class.newInstance();
      } catch (InstantiationException | IllegalAccessException e) {
        throw new RuntimeException("unable to get instance of Hive 2.x shim class");
      }
    }
    throw new RuntimeException("Shim class for Hive version " + hiveVersion + " does not exist");
  }

  @VisibleForTesting
  static synchronized void clearShimClass() {
    hiveShims = null;
  }
}
