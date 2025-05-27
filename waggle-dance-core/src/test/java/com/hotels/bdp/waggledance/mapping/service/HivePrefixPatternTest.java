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
package com.hotels.bdp.waggledance.mapping.service;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Test;

public class HivePrefixPatternTest {
  private static final String PREFIX = "waggle_";

  @Test
  public void hivePrefixPatternMatchDp() {
    boolean[] matched = HivePrefixPattern.matchDp(PREFIX, "wa...*sgdyu");
    checkMatched(matched, false, false, false, false, false, false,
        true, false, false, false, false, false);

    matched = HivePrefixPattern.matchDp(PREFIX, "wa.....sgdyu");
    checkMatched(matched, false, false, false, false, false, false,
        false, true, false, false, false, false, false);

    matched = HivePrefixPattern.matchDp(PREFIX, "wa.**.sgdyu");
    checkMatched(matched, false, false, false, false, true,
        true, true, false, false, false, false, false);

    matched = HivePrefixPattern.matchDp(PREFIX, "wa.**....sgdyu");
    checkMatched(matched, false, false, false, false, true, true,
        true, true, true, true, false, false, false, false, false);

    matched = HivePrefixPattern.matchDp("", "wa.**....sgdyu");
    checkMatched(matched, true, false, false, false, false, false,
        false, false, false, false, false, false, false, false, false);

    matched = HivePrefixPattern.matchDp("", "*wa.**....sgdyu");
    checkMatched(matched, true, true, false, false, false, false, false,
        false, false, false, false, false, false, false, false, false);
  }

  @Test
  public void hivePrefixPattern() {
    HivePrefixPattern hivePrefixPattern = new HivePrefixPattern(PREFIX, "wa...*sgdyu");
    assertEquals(Arrays.asList("*sgdyu"), hivePrefixPattern.getSubPatterns());

    hivePrefixPattern = new HivePrefixPattern(PREFIX, "wa.....sgdyu");
    assertEquals(Arrays.asList("sgdyu"), hivePrefixPattern.getSubPatterns());

    hivePrefixPattern = new HivePrefixPattern(PREFIX, "wa.**.sgdyu");
    assertEquals(Arrays.asList("**.sgdyu", "*.sgdyu", "sgdyu"), hivePrefixPattern.getSubPatterns());

    hivePrefixPattern = new HivePrefixPattern(PREFIX, "wa.**....sgdyu");
    assertEquals(Arrays.asList("**....sgdyu", "*....sgdyu", "...sgdyu", "..sgdyu", ".sgdyu", "sgdyu"),
        hivePrefixPattern.getSubPatterns());


    hivePrefixPattern = new HivePrefixPattern("", "wa.**....sgdyu");
    assertEquals(Arrays.asList("wa.**....sgdyu"),
        hivePrefixPattern.getSubPatterns());

    hivePrefixPattern = new HivePrefixPattern("", "*wa.**....sgdyu");
    assertEquals(Arrays.asList("*wa.**....sgdyu", "*wa.**....sgdyu"),
        hivePrefixPattern.getSubPatterns());

    hivePrefixPattern = new HivePrefixPattern(PREFIX, "*waggle_*...sgdyu");
    assertEquals(Arrays.asList("*waggle_*...sgdyu", "*...sgdyu", "*...sgdyu"),
        hivePrefixPattern.getSubPatterns());

    hivePrefixPattern = new HivePrefixPattern(PREFIX, "waggl*...sgdyu");
    assertEquals(Arrays.asList("*...sgdyu", "..sgdyu", ".sgdyu"),
        hivePrefixPattern.getSubPatterns());

    hivePrefixPattern = new HivePrefixPattern(PREFIX, "wag*..*...sgdyu");
    assertEquals(Arrays.asList("*..*...sgdyu", ".*...sgdyu", "*...sgdyu", "*...sgdyu", "..sgdyu", ".sgdyu"),
        hivePrefixPattern.getSubPatterns());

    hivePrefixPattern = new HivePrefixPattern(PREFIX, "waggle_sgdyu");
    assertEquals(Arrays.asList("sgdyu"),
        hivePrefixPattern.getSubPatterns());
  }

  private void checkMatched(boolean[] matched, boolean... expected) {
    for (int i = 0; i < matched.length; i++) {
      assertEquals(expected[i], matched[i]);
    }
  }
}