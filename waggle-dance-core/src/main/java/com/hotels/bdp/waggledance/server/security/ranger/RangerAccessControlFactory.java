/**
 * Copyright (C) 2016-2023 Expedia, Inc.
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
package com.hotels.bdp.waggledance.server.security.ranger;

import static com.hotels.bdp.waggledance.server.security.ranger.WaggleDanceRangerUtil.RANGER_REST_URL;

import org.apache.hadoop.hive.conf.HiveConf;

public class RangerAccessControlFactory {

  private static RangerAccessControlHandler instance;

  public static RangerAccessControlHandler getInstance(HiveConf conf) {
    if (instance == null) {
      synchronized (RangerAccessControlFactory.class) {
        if (instance == null) {
          String rangerRestUrl = conf.get(RANGER_REST_URL);
          if (rangerRestUrl == null || rangerRestUrl.isEmpty())
            instance = new DefaultRangerAccessControlHandler();
          else
            instance = new RangerAccessControlHandlerImpl(conf);
        }
      }
    }
    return instance;
  }

  private RangerAccessControlFactory() {}

}
