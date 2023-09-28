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

import java.util.Set;

public class DefaultRangerAccessControlHandler implements RangerAccessControlHandler {

  @Override
  public boolean hasUpdatePermission(String databaseName, String tableName, String user, Set<String> groups) {
    return true;
  }

  @Override
  public boolean hasCreatePermission(String databaseName, String tableName, String user, Set<String> groups) {
    return true;
  }

  @Override
  public boolean hasSelectPermission(String databaseName, String tableName, String user, Set<String> groups) {
    return true;
  }

  @Override
  public boolean hasDropPermission(String databaseName, String tableName, String user, Set<String> groups) {
    return true;
  }

  @Override
  public boolean hasAlterPermission(String databaseName, String tableName, String user, Set<String> groups) {
    return true;
  }

}
