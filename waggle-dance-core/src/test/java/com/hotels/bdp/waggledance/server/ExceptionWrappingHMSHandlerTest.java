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
package com.hotels.bdp.waggledance.server;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.server.security.NotAllowedException;

@RunWith(MockitoJUnitRunner.class)
public class ExceptionWrappingHMSHandlerTest {

  public @Rule ExpectedException expectedException = ExpectedException.none();

  private @Mock IHMSHandler baseHandler;

  @Test
  public void get_databaseNoExceptions() throws Exception {
    IHMSHandler handler = ExceptionWrappingHMSHandler.newProxyInstance(baseHandler);
    handler.get_database("bdp");
    verify(baseHandler).get_database("bdp");
  }

  @Test
  public void get_databaseWaggleDanceServerException() throws Exception {
    expectedException.expect(MetaException.class);
    IHMSHandler handler = ExceptionWrappingHMSHandler.newProxyInstance(baseHandler);
    when(baseHandler.get_database("bdp")).thenThrow(new WaggleDanceServerException("waggle waggle!"));
    handler.get_database("bdp");
  }

  @Test
  public void get_databasNotAllowedException() throws Exception {
    expectedException.expect(MetaException.class);
    IHMSHandler handler = ExceptionWrappingHMSHandler.newProxyInstance(baseHandler);
    when(baseHandler.get_database("bdp")).thenThrow(new NotAllowedException("waggle waggle!"));
    handler.get_database("bdp");
  }

  @Test
  public void get_databaseRunTimeExceptionIsNotWrapped() throws Exception {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("generic non waggle dance exception");
    IHMSHandler handler = ExceptionWrappingHMSHandler.newProxyInstance(baseHandler);
    when(baseHandler.get_database("bdp")).thenThrow(new RuntimeException("generic non waggle dance exception"));
    handler.get_database("bdp");
  }

  @Test
  public void get_databaseCheckedExceptionIsNotWrapped() throws Exception {
    expectedException.expect(NoSuchObjectException.class);
    expectedException.expectMessage("Does not exist!");
    IHMSHandler handler = ExceptionWrappingHMSHandler.newProxyInstance(baseHandler);
    when(baseHandler.get_database("bdp")).thenThrow(new NoSuchObjectException("Does not exist!"));
    handler.get_database("bdp");
  }
}
