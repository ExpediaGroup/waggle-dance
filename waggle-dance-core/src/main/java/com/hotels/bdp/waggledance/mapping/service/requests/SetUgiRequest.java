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
package com.hotels.bdp.waggledance.mapping.service.requests;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;

@AllArgsConstructor
public class SetUgiRequest implements RequestCallable<List<String>> {

  @Getter
  private final DatabaseMapping mapping;
  private final String user_name;
  private final List<String> group_names;

  @Override
  public List<String> call() throws Exception {
    return mapping.getClient().set_ugi(user_name, group_names);
  }
}
