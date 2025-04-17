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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;

import java.util.Map;

import org.hamcrest.collection.IsArrayWithSize;
import org.junit.Test;

import com.google.common.collect.ImmutableSet;

public class GrammarUtilsTest {

  private static final String PREFIX = "waggle_";

  @Test
  public void emptySubPattern() {
    String[] patternParts = GrammarUtils.splitPattern(PREFIX, "");
    assertThat(patternParts, IsArrayWithSize.emptyArray());
  }

  @Test
  public void basicSubPatternMatchingPrefix() {
    String[] patternParts = GrammarUtils.splitPattern(PREFIX, "waggle_");
    assertThat(patternParts[0], is(PREFIX));
    assertThat(patternParts[1], is(""));
  }

  @Test
  public void basicSubPatternNotMatchingPrefix() {
    String[] patternParts = GrammarUtils.splitPattern("prefix", "waggle_");
    assertThat(patternParts, IsArrayWithSize.emptyArray());
  }

  @Test
  public void subPatternMatchesEverything() {
    String[] patternParts = GrammarUtils.splitPattern(PREFIX, "*");
    assertThat(patternParts[0], is("*"));
    assertThat(patternParts[1], is("*"));
  }

  @Test
  public void subPatternMatchesAllTables() {
    String[] patternParts = GrammarUtils.splitPattern(PREFIX, "waggle_*");
    assertThat(patternParts[0], is(PREFIX));
    assertThat(patternParts[1], is("*"));
  }

  @Test
  public void subPatternMatchesAllSpecificTables() {
    String[] patternParts = GrammarUtils.splitPattern(PREFIX, "waggle_*base");
    assertThat(patternParts[0], is(PREFIX));
    assertThat(patternParts[1], is("*base"));
  }

  @Test
  public void subPatternMatchesDatabaseAndAllSpecificTables() {
    String[] patternParts = GrammarUtils.splitPattern(PREFIX, "wag*base");
    assertThat(patternParts[0], is("wag*"));
    assertThat(patternParts[1], is("*base"));
  }

  @Test
  public void splitPatternWithDot() {
    String[] patternParts = GrammarUtils.splitPattern(PREFIX, "waggle.dm*");
    assertThat(patternParts[0], is("waggle."));
    assertThat(patternParts[1], is("dm*"));
  }

  @Test
  public void splitPatternWithDots() {
    String[] patternParts = GrammarUtils.splitPattern(PREFIX, "waggle...dm");
    assertThat(patternParts[0], is("waggle."));
    assertThat(patternParts[1], is("..dm"));
  }

  @Test
  public void splitPatternWithDotsAndStarEnd() {
    String[] patternParts = GrammarUtils.splitPattern(PREFIX, "waggle...dm*");
    assertThat(patternParts[0], is("waggle."));
    assertThat(patternParts[1], is("..dm*"));
  }

  @Test
  public void splitPatternWithDotsAndStar() {
    String[] patternParts = GrammarUtils.splitPattern(PREFIX, "waggle.*.dm");
    assertThat(patternParts[0], is("waggle.*"));
    assertThat(patternParts[1], is("*.dm"));
  }

  @Test
  public void splitPatternWithDotsInMiddle() {
    String[] patternParts = GrammarUtils.splitPattern(PREFIX, "wa..le_dm");
    assertThat(patternParts[0], is("wa..le_"));
    assertThat(patternParts[1], is("dm"));
  }

  @Test
  public void matchesWithNullPattern() {
    Map<String, String> splits = GrammarUtils.selectMatchingPrefixes(ImmutableSet.of(PREFIX, "other_"), null);
    assertThat(splits.size(), is(2));
    assertNull(splits.get(PREFIX));
    assertNull(splits.get("other_"));
  }

  @Test
  public void matchesWithWildcardPattern() {
    Map<String, String> splits = GrammarUtils.selectMatchingPrefixes(ImmutableSet.of(PREFIX, "other_"), "*");
    assertThat(splits.size(), is(2));
    assertThat(splits.get(PREFIX), is("*"));
    assertThat(splits.get("other_"), is("*"));
  }

  @Test
  public void matchesWithDotWildcardPattern() {
    Map<String, String> splits = GrammarUtils.selectMatchingPrefixes(ImmutableSet.of(PREFIX, "other_"), "other.dm");
    assertThat(splits.size(), is(1));
    assertThat(splits.get("other_"), is("dm"));

    splits = GrammarUtils.selectMatchingPrefixes(ImmutableSet.of(PREFIX), "waggle*.dm");
    assertThat(splits.size(), is(1));
    assertThat(splits.get(PREFIX), is("dm|*.dm"));
  }

  @Test
  public void doesNotMatchPatternSimpleDatabaseName() {
    Map<String, String> splits = GrammarUtils.selectMatchingPrefixes(ImmutableSet.of(PREFIX, "other_"), "database");
    assertThat(splits.size(), is(0));
  }

  @Test
  public void matchesPatternSimplePrefixedDatabaseName() {
    Map<String, String> splits = GrammarUtils
        .selectMatchingPrefixes(ImmutableSet.of(PREFIX, "other_"), "waggle_database");
    assertThat(splits.size(), is(1));
    assertThat(splits.get(PREFIX), is("database"));
  }

  @Test
  public void matchesPatternWithPrefixAndWildcard() {
    Map<String, String> splits = GrammarUtils.selectMatchingPrefixes(ImmutableSet.of(PREFIX, "other_"), "waggle_d*");
    assertThat(splits.size(), is(1));
    assertThat(splits.get(PREFIX), is("d*"));
  }

  @Test
  public void matchesPatternWithWildcard() {
    Map<String, String> splits = GrammarUtils.selectMatchingPrefixes(ImmutableSet.of(PREFIX, "other_"), "wagg*base");
    assertThat(splits.size(), is(1));
    assertThat(splits.get(PREFIX), is("*base"));
  }

  @Test
  public void matchesAllPatternWithWildcard() {
    Map<String, String> splits = GrammarUtils.selectMatchingPrefixes(ImmutableSet.of(PREFIX, "wother_"), "w*base");
    assertThat(splits.size(), is(2));
    assertThat(splits.get(PREFIX), is("*base"));
    assertThat(splits.get("wother_"), is("*base"));
  }

  @Test
  public void matchesComplexPatternWithWildcard() {
    Map<String, String> splits = GrammarUtils
        .selectMatchingPrefixes(ImmutableSet.of(PREFIX, "other_"), "w*base|oth*_*dat");
    assertThat(splits.size(), is(2));
    assertThat(splits.get(PREFIX), is("*base"));
    assertThat(splits.get("other_"), is("*_*dat|*dat"));
  }

  @Test
  public void multipleMatchesComplexPatternWithWildcard() {
    Map<String, String> splits = GrammarUtils
        .selectMatchingPrefixes(ImmutableSet.of(PREFIX, "wother_"), "w*base|woth*_*dat");
    assertThat(splits.size(), is(2));
    assertThat(splits.get(PREFIX), is("*base"));
    assertThat(splits.get("wother_"), is("*base|*_*dat|*dat"));
  }

  @Test
  public void multipleMatchesPatternWithMultipleWildcard() {
    Map<String, String> splits = GrammarUtils.selectMatchingPrefixes(ImmutableSet.of(PREFIX, "baggle_"), "*aggle*");
    assertThat(splits.size(), is(2));
    assertThat(splits.get(PREFIX), is("*|*aggle*"));
    assertThat(splits.get("baggle_"), is("*|*aggle*"));
  }

  @Test
  public void matchesPatternWithFullPrefix() {
    Map<String, String> splits = GrammarUtils.selectMatchingPrefixes(
        ImmutableSet.of(PREFIX, "baggle_"), PREFIX + "|" + "baggle_");
    assertThat(splits.size(), is(0));
  }
}
