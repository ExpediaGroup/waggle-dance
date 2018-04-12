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
package com.hotels.bdp.waggledance.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class WhitelistTest {

  @Test
  public void add() {
    Whitelist whitelist = new Whitelist();
    whitelist.add("db");
    assertEquals(1, whitelist.size());
  }

  @Test
  public void isEmpty() {
    Whitelist whitelist = new Whitelist();
    assertTrue(whitelist.isEmpty());
    whitelist.add("db");
    assertFalse(whitelist.isEmpty());
  }

  @Test
  public void containsTrue() {
    Whitelist whitelist = new Whitelist(ImmutableList.<String> of("db_.*", "user"));
    assertTrue(whitelist.contains("db_test"));
    assertTrue(whitelist.contains("user"));
  }

  @Test
  public void containsFalse() {
    Whitelist whitelist = new Whitelist(ImmutableList.<String> of("db_.*", "user"));
    assertFalse(whitelist.contains("foo"));
    assertFalse(whitelist.contains("users"));
  }
}
