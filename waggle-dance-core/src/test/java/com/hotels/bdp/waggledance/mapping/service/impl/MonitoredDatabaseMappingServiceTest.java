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
package com.hotels.bdp.waggledance.mapping.service.impl;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.mapping.model.DatabaseMapping;
import com.hotels.bdp.waggledance.mapping.service.MappingEventListener;
import com.hotels.bdp.waggledance.mapping.service.PanopticOperationHandler;
import com.hotels.bdp.waggledance.metrics.CurrentMonitoredMetaStoreHolder;

@RunWith(MockitoJUnitRunner.class)
public class MonitoredDatabaseMappingServiceTest {

  private @Mock MappingEventListener wrapped;
  private @Mock DatabaseMapping primaryMapping;
  private @Mock DatabaseMapping otherMapping;
  private @Mock PanopticOperationHandler multiMetastoreOperationsHandler;
  private @Mock AbstractMetaStore metaStore;
  private MonitoredDatabaseMappingService service;

  @Before
  public void init() {
    when(primaryMapping.getMetastoreMappingName()).thenReturn("primary");
    when(otherMapping.getMetastoreMappingName()).thenReturn("other");
    when(wrapped.primaryDatabaseMapping()).thenReturn(primaryMapping);
    when(wrapped.databaseMapping(anyString())).thenReturn(otherMapping);
    when(wrapped.getPanopticOperationHandler()).thenReturn(multiMetastoreOperationsHandler);
    service = new MonitoredDatabaseMappingService(wrapped);
    CurrentMonitoredMetaStoreHolder.monitorMetastore("notSet");
  }

  @Test
  public void primaryDatabaseMapping() {
    assertThat(service.primaryDatabaseMapping(), is(primaryMapping));
    verify(wrapped).primaryDatabaseMapping();
    assertThat(CurrentMonitoredMetaStoreHolder.getMonitorMetastore(), is("primary"));
  }

  @Test
  public void databaseMapping() {
    assertThat(service.databaseMapping("other"), is(otherMapping));
    verify(wrapped).databaseMapping("other");
    assertThat(CurrentMonitoredMetaStoreHolder.getMonitorMetastore(), is("other"));
  }

  @Test
  public void getMultiMetaStoreOperationsHandler() {
    assertThat(service.getPanopticOperationHandler(), is(multiMetastoreOperationsHandler));
    verify(wrapped).getPanopticOperationHandler();
    assertThat(CurrentMonitoredMetaStoreHolder.getMonitorMetastore(), is("all"));
  }

  @Test
  public void onRegister() throws Exception {
    service.onRegister(metaStore);
    verify(wrapped).onRegister(metaStore);
  }

  @Test
  public void onUnregister() throws Exception {
    service.onUnregister(metaStore);
    verify(wrapped).onUnregister(metaStore);
  }

  @Test
  public void onUpdate() throws Exception {
    AbstractMetaStore newMetastore = Mockito.mock(AbstractMetaStore.class);
    service.onUpdate(metaStore, newMetastore);
    verify(wrapped).onUpdate(metaStore, newMetastore);
  }

  @Test
  public void close() throws Exception {
    service.close();
    verify(wrapped).close();
  }
}
