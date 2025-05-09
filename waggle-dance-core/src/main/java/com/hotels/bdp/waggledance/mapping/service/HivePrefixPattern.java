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

import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

public class HivePrefixPattern {
  private String prefix;
  private String pattern;
  private final List<String> subPatterns = new ArrayList<>();

  /**
   * Using a dynamic programming algorithm to match a given pattern with an input string,
   * where '*' represents matching any number of any characters,
   * and '.' represents matching a single arbitrary character.
   *
   * @param input input string.
   * @param pattern regular expression.
   * @return A boolean array with a length of one more than the length of the pattern.
   * If the substring of the pattern from index 0 to i+1 can match the input string,
   * the value at index i of the boolean array is true; otherwise, it is false.
   */
  @VisibleForTesting
  static boolean[] matchDp(String input, String pattern) {
    int m = input.length() + 1;
    int n = pattern.length() + 1;
    boolean[] matchingResult = new boolean[n];
    boolean[] prevMatchingResult = new boolean[n];

    matchingResult[0] = true;

    for (int j = 1; j < n; j++) {
      matchingResult[j] = matchingResult[j - 1] && pattern.charAt(j - 1) == '*';
    }

    for (int i = 1; i < m; i++) {
      copy(prevMatchingResult, matchingResult);
      matchingResult[0] = false;
      for (int j = 1; j < n; j++) {
        if (pattern.charAt(j - 1) == '*') {
          matchingResult[j] = prevMatchingResult[j] || prevMatchingResult[j - 1] || matchingResult[j - 1];
        } else {
          matchingResult[j] = prevMatchingResult[j - 1] && (pattern.charAt(j - 1) == '.'
              || input.charAt(i - 1) == pattern.charAt(j - 1));
        }
      }
    }
    return matchingResult;
  }

  static void copy(boolean[] prev, boolean[] current) {
    for (int i = 0; i < prev.length; i++) {
      prev[i] = current[i];
    }
  }

  public HivePrefixPattern(String prefix, String pattern) {
    if (prefix == null || pattern == null) {
      return;
    }
    this.prefix = prefix;
    this.pattern = pattern;
    init();
  }

  private void init() {
    boolean[] match = matchDp(this.prefix, this.pattern);
    if (match[0]) {
      this.subPatterns.add(this.pattern);
    }
    int n = this.pattern.length();
    for (int j = 1; j <= n; j++) {
      if (!match[j]) {
        continue;
      }
      String subPattern = this.pattern.charAt(j - 1) == '*' ?
          this.pattern.substring(j - 1, n) : this.pattern.substring(j, n);
      if (subPattern.isEmpty()) {
        continue;
      }
      this.subPatterns.add(subPattern);
    }
  }

  public String getPrefix() {
    return prefix;
  }

  public String getPattern() {
    return pattern;
  }

  public List<String> getSubPatterns() {
    return subPatterns;
  }

  @Override
  public String toString() {
    return "HivePrefixPattern{" +
        "prefix='" + prefix + '\'' +
        ", pattern='" + pattern + '\'' +
        ", subPatterns=" + subPatterns +
        '}';
  }
}
