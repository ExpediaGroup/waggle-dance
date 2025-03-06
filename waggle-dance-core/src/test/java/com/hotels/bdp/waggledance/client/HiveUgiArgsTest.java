/**
 * Copyright (C) 2016-2025 Expedia, Inc.
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
package com.hotels.bdp.waggledance.client;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;

import org.junit.Test;

public class HiveUgiArgsTest {

  @Test
  public void groups() throws Exception {
    HiveUgiArgs args = new HiveUgiArgs("user", new ArrayList<>());
    assertThat("user", is(args.getUser()));
    asssertThatListIsMutatable(args);
  }

  private void asssertThatListIsMutatable(HiveUgiArgs args) {
    assertThat(args.getGroups().size(), is(0));
    // List should be mutable, Hive code potentially mutates it.
    args.getGroups().add("user");
    assertThat(args.getGroups().size(), is(1));
  }

  @Test
  public void groupDefaults() throws Exception {
    HiveUgiArgs args = HiveUgiArgs.WAGGLE_DANCE_DEFAULT;
    assertThat("waggledance", is(args.getUser()));
    asssertThatListIsMutatable(args);
  }

  @Test
  public void groupsImmutable() throws Exception {
    HiveUgiArgs args = new HiveUgiArgs("user", Collections.emptyList());
    assertThat("user", is(args.getUser()));
    asssertThatListIsMutatable(args);
  }

  @Test
  public void groupsNull() throws Exception {
    HiveUgiArgs args = new HiveUgiArgs("user", null);
    assertThat("user", is(args.getUser()));
    assertTrue(args.getGroups().isEmpty());
  }

}
