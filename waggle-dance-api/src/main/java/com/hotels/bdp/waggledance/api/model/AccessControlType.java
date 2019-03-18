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
package com.hotels.bdp.waggledance.api.model;

public enum AccessControlType {

  /**
   * Only read operations are allowed
   */
  READ_ONLY,

  /**
   * Allows the following:
   * <ul>
   * <li>All read operations are allowed</li>
   * <li>Write operations are allowed</li>
   * <li>Database creation is allowed</li>
   * </ul>
   */
  READ_AND_WRITE_AND_CREATE,

  /**
   * Allows the following:
   * <ul>
   * <li>All read operations are allowed</li>
   * <li>Write operations are allowed if the database in question is present in the configured whitelist</li>
   * <li>Database creation is not allowed</li>
   * </ul>
   */
  READ_AND_WRITE_ON_DATABASE_WHITELIST,

  /**
   * Allows the following:
   * <ul>
   * <li>All read operations are allowed</li>
   * <li>Write operations are allowed if the database in question is present in the configured whitelist</li>
   * <li>Database creation is allowed, Created databases will be added to the whitelist automatically and will be
   * writable</li>
   * </ul>
   */
  READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST

}
