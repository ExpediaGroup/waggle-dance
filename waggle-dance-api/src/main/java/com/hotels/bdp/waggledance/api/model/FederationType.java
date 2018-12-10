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
package com.hotels.bdp.waggledance.api.model;

/**
 * Defines the type of federation we provide over a metastore, provides two flavours:
 *
 * <pre>
 * PRIMARY: only one metastore can be the primary metastore, database agnostic operations are allowed. Read/write/create
 * operations are configured per database (See {@link AccessControlType}).
 * </pre>
 *
 * <pre>
 * FEDERATED: normal database related operations are allowed, database agnostic operations are not. Write/Create
 * operations are not allowed.
 * </pre>
 **/
public enum FederationType {
  PRIMARY,
  FEDERATED
}
