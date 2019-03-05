/**
 * Copyright (C) 2016-2018 Expedia, Inc.
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
package com.hotels.bdp.waggledance.server.security;

public interface AccessControlHandler {

  /**
   * @param databaseName name of the database
   * @return true if databaseName is writable, false otherwise
   */
  boolean hasWritePermission(String databaseName);

  /**
   * @return true if databases are allowed to be created, false otherwise
   */
  boolean hasCreatePermission();

  /**
   * Called when a database was created via waggleDance, can be used to update any permissions
   *
   * @param name of the created database
   */
  void databaseCreatedNotification(String name);
}
