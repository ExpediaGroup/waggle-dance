/**
 * Copyright (C) 2016-2020 Expedia, Inc.
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

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AllowList {

  private final static String MATCH_ALL = ".*";
  private final Set<Pattern> allowList = new HashSet<>();

  public AllowList() {}

  public AllowList(List<String> allowList) {
    if (allowList == null) {
      add(MATCH_ALL);
    } else {
      for (String element : allowList) {
        add(element);
      }
    }
  }

  public void add(String element) {
    allowList.add(Pattern.compile(trimToLowerCase(element)));
  }

  int size() {
    return allowList.size();
  }

  private String trimToLowerCase(String string) {
    return string.trim().toLowerCase(Locale.ROOT);
  }

  public boolean contains(String element) {
    if (element == null) {
      return true;
    }
    element = trimToLowerCase(element);
    for (Pattern allowListEntry : allowList) {
      Matcher matcher = allowListEntry.matcher(element);
      if (matcher.matches()) {
        return true;
      }
    }
    return false;
  }

}
