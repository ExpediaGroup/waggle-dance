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

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;

import lombok.AllArgsConstructor;
import lombok.Getter;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;

@AllArgsConstructor
public class GetAllFunctionsRequest implements RequestCallable<List<GetAllFunctionsResponse>> {

  @Getter
  private final DatabaseMapping mapping;

  @Override
  public List<GetAllFunctionsResponse> call() throws Exception {
    GetAllFunctionsResponse response = mapping.getClient().get_all_functions();
    if (response.getFunctions() != null) {
      for (Function function : response.getFunctions()) {
        mapping.transformOutboundFunction(function);
      }
    }
    return Collections.singletonList(response);
  }

}
