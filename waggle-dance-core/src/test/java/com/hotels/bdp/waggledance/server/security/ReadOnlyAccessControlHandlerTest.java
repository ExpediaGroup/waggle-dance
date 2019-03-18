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
package com.hotels.bdp.waggledance.server.security;

import static org.junit.Assert.assertFalse;

import org.junit.Test;

public class ReadOnlyAccessControlHandlerTest {

  @Test
  public void hasWritePermission() {
    ReadOnlyAccessControlHandler handler = new ReadOnlyAccessControlHandler();
    assertFalse(handler.hasWritePermission("db"));
    assertFalse(handler.hasWritePermission(null));
  }

  @Test
  public void hasCreatePermission() {
    ReadOnlyAccessControlHandler handler = new ReadOnlyAccessControlHandler();
    assertFalse(handler.hasCreatePermission());
  }

  @Test
  public void databaseCreatedNotification() {
    ReadOnlyAccessControlHandler handler = new ReadOnlyAccessControlHandler();
    // nothing should happen
    handler.databaseCreatedNotification("db");
  }
}
