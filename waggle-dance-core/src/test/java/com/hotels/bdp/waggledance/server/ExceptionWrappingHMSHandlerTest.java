/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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

import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.server.security.NotAllowedException;

@RunWith(MockitoJUnitRunner.class)
public class ExceptionWrappingHMSHandlerTest {

  private @Mock IHMSHandler baseHandler;

  @Test
  public void get_databaseNoExceptions() throws Exception {
    IHMSHandler handler = ExceptionWrappingHMSHandler.newProxyInstance(baseHandler);
    handler.get_database("bdp");
    verify(baseHandler).get_database("bdp");
  }

  @Test
  public void get_databaseWaggleDanceServerException() throws Exception {
    IHMSHandler handler = ExceptionWrappingHMSHandler.newProxyInstance(baseHandler);
    when(baseHandler.get_database("bdp")).thenThrow(new WaggleDanceServerException("waggle waggle!"));
    assertThrows(MetaException.class, () -> { handler.get_database("bdp");});
  }

  @Test
  public void get_databasNotAllowedException() throws Exception {
    IHMSHandler handler = ExceptionWrappingHMSHandler.newProxyInstance(baseHandler);
    when(baseHandler.get_database("bdp")).thenThrow(new NotAllowedException("waggle waggle!"));
    assertThrows(MetaException.class, () -> { handler.get_database("bdp");});
  }

  @Test
  public void get_databaseRunTimeExceptionIsNotWrapped() throws Exception {
    IHMSHandler handler = ExceptionWrappingHMSHandler.newProxyInstance(baseHandler);
    when(baseHandler.get_database("bdp")).thenThrow(new RuntimeException("generic non waggle dance exception"));
    assertThrows("generic non waggle dance exception",RuntimeException.class, () -> { handler.get_database("bdp");});
  }

  @Test
  public void get_databaseCheckedExceptionIsNotWrapped() throws Exception {
    IHMSHandler handler = ExceptionWrappingHMSHandler.newProxyInstance(baseHandler);
    when(baseHandler.get_database("bdp")).thenThrow(new NoSuchObjectException("Does not exist!"));
    assertThrows("Does not exist!",NoSuchObjectException.class, () -> { handler.get_database("bdp");});
  }
}
