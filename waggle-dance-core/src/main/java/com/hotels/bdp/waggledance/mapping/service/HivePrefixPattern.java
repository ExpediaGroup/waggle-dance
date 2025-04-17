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
  private final String prefix;
  private String pattern;
  private final List<String> subPatterns;

  @VisibleForTesting
  static boolean[] matchDp(String input, String pattern) {
    int m = input.length() + 1;
    int n = pattern.length() + 1;
    boolean[] dp = new boolean[n];
    boolean[] prev = new boolean[n];

    dp[0] = true;

    for (int j = 1; j < n; j++) {
      dp[j] = dp[j - 1] && pattern.charAt(j - 1) == '*';
    }

    for (int i = 1; i < m; i++) {
      copy(prev, dp);
      dp[0] = false;
      for (int j = 1; j < n; j++) {
        dp[j] = pattern.charAt(j - 1) == '*' ?
            prev[j] || prev[j - 1] || dp[j - 1] :
            prev[j - 1] && (pattern.charAt(j - 1) == '.'
                || input.charAt(i - 1) == pattern.charAt(j - 1));
      }
    }
    return dp;
  }

  static void copy(boolean[] prev, boolean[] dp) {
    for (int i = 0; i < prev.length; i++) {
      prev[i] = dp[i];
    }
  }

  public HivePrefixPattern(String prefix, String pattern) {
    this.prefix = prefix;
    this.pattern = pattern;
    this.subPatterns = new ArrayList<>();
    init();
  }

  private void init() {
    if (this.prefix == null || this.pattern == null) {
      return;
    }
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
