/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Collections;

import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class AllowListTest {

  @Test
  public void add() {
    AllowList allowList = new AllowList();
    allowList.add("db");
    assertThat(allowList.size(), is(1));
  }

  @Test
  public void containsTrue() {
    AllowList allowList = new AllowList(ImmutableList.of("db_.*", "user"));
    assertTrue(allowList.contains("db_test"));
    assertTrue(allowList.contains("user"));
  }

  @Test
  public void containsFalse() {
    AllowList allowList = new AllowList(ImmutableList.of("db_.*", "user"));
    assertFalse(allowList.contains("foo"));
    assertFalse(allowList.contains("users"));
  }

  @Test
  public void addNull() {
    AllowList allowList = new AllowList(null);
    assertThat(allowList.size(), is(1));
    assertThat(allowList.contains("abc"), is(true));
  }

  @Test
  public void addEmpty() {
    AllowList allowList = new AllowList(Collections.emptyList());
    assertThat(allowList.size(), is(0));
    assertThat(allowList.contains("abs"), is(false));
  }
}
