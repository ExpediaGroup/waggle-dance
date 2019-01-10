/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;

public final class GrammarUtils {

  private static final String OR_SEPARATOR = "|";
  private static final Splitter OR_SPLITTER = Splitter.on(OR_SEPARATOR);
  private static final Joiner OR_JOINER = Joiner.on(OR_SEPARATOR);

  private GrammarUtils() {}

  @VisibleForTesting
  static String[] splitPattern(String prefix, String pattern) {
    if (pattern.startsWith(prefix)) {
      return new String[] { prefix, pattern.substring(prefix.length()) };
    }

    // Find the longest sub-pattern that matches the prefix
    String subPattern = pattern;
    int index = pattern.length();
    while (index >= 0) {
      String subPatternRegex = subPattern.replaceAll("\\*", ".*");
      if (prefix.matches(subPatternRegex)) {
        return new String[] { subPattern, pattern.substring(subPattern.length() - 1) };
      }
      // Skip last * and find the next sub-pattern
      if (subPattern.endsWith("*")) {
        subPattern = subPattern.substring(0, subPattern.length() - 1);
      }
      index = subPattern.lastIndexOf("*");
      if (index >= 0) {
        subPattern = subPattern.substring(0, index + 1);
      }
    }
    return new String[] {};
  }

  /**
   * Selects Waggle Dance database mappings that can potentially match the provided pattern.
   * <p>
   * This implementation is using {@link org.apache.hadoop.hive.metastore.ObjectStore#getDatabases(String)} as reference
   * for pattern matching.
   * <p>
   * To learn more about Hive DDL patterns refer to
   * <a href="https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Show">Language
   * Manual</a> for details
   *
   * @param prefixes Federation prefixes
   * @param dbPatterns Database name patters
   * @return A map of possible database prefixes to be used for interrogation with their pattern
   */
  public static Map<String, String> selectMatchingPrefixes(Set<String> prefixes, String dbPatterns) {
    Map<String, String> matchingPrefixes = new HashMap<>();
    if ((dbPatterns == null) || "*".equals(dbPatterns)) {
      for (String prefix : prefixes) {
        matchingPrefixes.put(prefix, dbPatterns);
      }
      return matchingPrefixes;
    }

    Map<String, List<String>> prefixPatterns = new HashMap<>();
    for (String subPattern : OR_SPLITTER.split(dbPatterns)) {
      for (String prefix : prefixes) {
        String[] subPatternParts = splitPattern(prefix, subPattern);
        if (subPatternParts.length == 0) {
          continue;
        }
        List<String> prefixPatternList = prefixPatterns.get(prefix);
        if (prefixPatternList == null) {
          prefixPatternList = new ArrayList<>();
          prefixPatterns.put(prefix, prefixPatternList);
        }
        prefixPatternList.add(subPatternParts[1]);
      }
    }

    for (Entry<String, List<String>> prefixPatternEntry : prefixPatterns.entrySet()) {
      matchingPrefixes.put(prefixPatternEntry.getKey(), OR_JOINER.join(prefixPatternEntry.getValue()));
    }
    return matchingPrefixes;
  }

}
