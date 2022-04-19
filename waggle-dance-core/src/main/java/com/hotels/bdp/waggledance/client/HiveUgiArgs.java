/**
 * Copyright (C) 2016-2022 Expedia, Inc.
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

import java.util.Collections;
import java.util.List;

public class HiveUgiArgs {

  public static final HiveUgiArgs WAGGLE_DANCE_DEFAULT = new HiveUgiArgs("waggledance", Collections.emptyList());

  private final String user;
  private final List<String> groups;

  public HiveUgiArgs(String user, List<String> groups) {
    this.user = user;
    this.groups = groups;
  }

  public String getUser() {
    return user;
  }

  public List<String> getGroups() {
    return groups;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((groups == null) ? 0 : groups.hashCode());
    result = prime * result + ((user == null) ? 0 : user.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    HiveUgiArgs other = (HiveUgiArgs) obj;
    if (groups == null) {
      if (other.groups != null) {
        return false;
      }
    } else if (!groups.equals(other.groups)) {
      return false;
    }
    if (user == null) {
      if (other.user != null) {
        return false;
      }
    } else if (!user.equals(other.user)) {
      return false;
    }
    return true;
  }

}
