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

public class WaggleDanceRangerUtil {

  // Prefix
  public static final String SERVICE_TYPE = "waggle-dance";
  public static final String PROPERTY_PREFIX = "ranger.plugin." + SERVICE_TYPE;

  // Ranger properties
  public static final String RANGER_REST_URL = PROPERTY_PREFIX + ".policy.rest.url";
  public static final String CACHE_DIR = PROPERTY_PREFIX + ".policy.cache.dir";
  public static final String RANGER_SERVICE_NAME = PROPERTY_PREFIX + ".service.name";
  public static final String RANGER_SOURCE_IMPL = PROPERTY_PREFIX + ".policy.source.impl";
  public static final String RANGER_POLICY_POLLINTERVAL = PROPERTY_PREFIX + ".policy.pollIntervalMs";

}
