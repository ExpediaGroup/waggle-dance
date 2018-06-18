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
package com.hotels.bdp.waggledance.server.glue.converter.util;

import java.util.Collection;

public class LoggingHelper
{
  private static final int MAX_LOG_STRING_LEN = 2000;

  public static String concatCollectionToStringForLogging(Collection<String> collection, String delimiter)
  {
    if (collection == null) {
      return "";
    }
    if (delimiter == null) {
      delimiter = ",";
    }
    StringBuilder bldr = new StringBuilder();
    int totalLen = 0;
    int delimiterSize = delimiter.length();
    for (String str : collection)
    {
      if (totalLen > 2000) {
        break;
      }
      if (str.length() + totalLen > 2000)
      {
        bldr.append(str.subSequence(0, 2000 - totalLen));
        break;
      }
      bldr.append(str);
      bldr.append(delimiter);
      totalLen += str.length() + delimiterSize;
    }
    return bldr.toString();
  }
}
