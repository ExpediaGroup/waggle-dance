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
package com.hotels.bdp.waggledance.mapping.service.requests;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.stubs.HiveStubs.newFunction;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;

@RunWith(MockitoJUnitRunner.class)
public class GetAllFunctionsRequestTest {

  @Mock
  private DatabaseMapping mapping;
  @Mock
  private Iface client;

  @Test
  public void call() throws Exception {
    GetAllFunctionsRequest request = new GetAllFunctionsRequest(mapping);
    GetAllFunctionsResponse response = new GetAllFunctionsResponse();
    Function function = newFunction("db", "fn1");
    response.addToFunctions(function);

    when(mapping.getClient()).thenReturn(client);
    when(mapping.getClient().get_all_functions()).thenReturn(response);
    List<GetAllFunctionsResponse> responses = request.call();
    assertThat(responses.size(), is(1));
    assertThat(responses.get(0), is(response));
    verify(mapping).transformOutboundFunction(function);
  }

  @Test
  public void callFunctionsNotSet() throws Exception {
    GetAllFunctionsRequest request = new GetAllFunctionsRequest(mapping);
    GetAllFunctionsResponse response = new GetAllFunctionsResponse();

    when(mapping.getClient()).thenReturn(client);
    when(mapping.getClient().get_all_functions()).thenReturn(response);
    List<GetAllFunctionsResponse> responses = request.call();
    assertThat(responses.size(), is(1));
    assertThat(responses.get(0), is(response));
    verify(mapping, never()).transformOutboundFunction(any(Function.class));
  }

  @Test
  public void callFunctionsEmpty() throws Exception {
    // Not 100% sure if this actually can happen but we cover it anyway.
    GetAllFunctionsRequest request = new GetAllFunctionsRequest(mapping);
    GetAllFunctionsResponse response = new GetAllFunctionsResponse();
    response.setFunctions(Collections.emptyList());

    when(mapping.getClient()).thenReturn(client);
    when(mapping.getClient().get_all_functions()).thenReturn(response);
    List<GetAllFunctionsResponse> responses = request.call();
    assertThat(responses.size(), is(1));
    assertThat(responses.get(0), is(response));
    verify(mapping, never()).transformOutboundFunction(any(Function.class));
  }

  @Test
  public void getMapping() throws Exception {
    GetAllFunctionsRequest request = new GetAllFunctionsRequest(mapping);
    assertThat(request.getMapping(), is(mapping));
  }

}
