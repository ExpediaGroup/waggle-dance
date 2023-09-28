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
package com.hotels.bdp.waggledance.server.security.ranger;

import java.util.Date;
import java.util.Set;

import org.apache.ranger.plugin.policyengine.RangerAccessRequestImpl;
import org.apache.ranger.plugin.policyengine.RangerAccessResource;

public class AccessRequest extends RangerAccessRequestImpl {
  private AccessType accessType;
  private RangerAccessResource resource;

  public AccessRequest(RangerAccessResource resource, AccessType accessType, String user, Set<String> groups) {
    this();
    this.resource = resource;
    this.accessType = accessType;
    this.setResource(resource);
    this.setAccessType(accessType.name().toLowerCase());
    this.setAction(accessType.toString());
    this.setUser(user);
    this.setUserGroups(groups);
    this.setAccessTime(new Date());
  }

  private AccessRequest() {
    super();
  }

}
