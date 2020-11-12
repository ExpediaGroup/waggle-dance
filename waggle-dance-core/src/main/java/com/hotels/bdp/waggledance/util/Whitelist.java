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
package com.hotels.bdp.waggledance.util;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Whitelist {

  private final static String MATCH_ALL = ".*";
  private final Set<Pattern> whiteList = new HashSet<>();

  public Whitelist() {}

  public Whitelist(List<String> whiteList) {
    if (whiteList == null) {
      add(MATCH_ALL);
    } else {
      for (String element : whiteList) {
        add(element);
      }
    }
  }

  public void add(String element) {
    whiteList.add(Pattern.compile(trimToLowerCase(element)));
  }

  int size() {
    return whiteList.size();
  }

  private String trimToLowerCase(String string) {
    return string.trim().toLowerCase(Locale.ROOT);
  }

  public boolean contains(String element) {
    if (element == null) {
      return true;
    }
    element = trimToLowerCase(element);
    for (Pattern whiteListEntry : whiteList) {
      Matcher matcher = whiteListEntry.matcher(element);
      if (matcher.matches()) {
        return true;
      }
    }
    return false;
  }

}
