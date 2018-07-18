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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;

import java.util.Arrays;
import java.util.List;

import javax.validation.ValidationException;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.mapping.service.FederatedMetaStoreStorage;
import com.hotels.bdp.waggledance.mapping.service.impl.NotifyingFederationService.FederationEventListener;

@RunWith(MockitoJUnitRunner.class)
public class NotifyingFederationServiceTest {

  private static final String METASTORE_NAME = "name";
  private static final String URI = "uri";

  private @Mock FederatedMetaStoreStorage federatedMetaStoreStorage;
  private @Mock FederationEventListener federationEventListener;

  private NotifyingFederationService service;

  @Before
  public void init() throws Exception {
    AbstractMetaStore federatedMetaStore = newFederatedInstance(METASTORE_NAME, URI);
    when(federatedMetaStoreStorage.getAll()).thenReturn(Arrays.asList(federatedMetaStore));
    when(federatedMetaStoreStorage.delete(METASTORE_NAME)).thenReturn(federatedMetaStore);
    when(federatedMetaStoreStorage.get(METASTORE_NAME)).thenReturn(federatedMetaStore);

    service = new NotifyingFederationService(federatedMetaStoreStorage);
    service.subscribe(federationEventListener);
  }

  @Test
  public void postContruct() {
    service.postConstruct();
    verify(federationEventListener).onRegister(newFederatedInstance(METASTORE_NAME, URI));
  }

  @Test
  public void preDestroy() {
    service.preDestroy();
    verify(federationEventListener).onUnregister(newFederatedInstance(METASTORE_NAME, URI));
  }

  @Test
  public void register() {
    AbstractMetaStore federatedMetaStore = newFederatedInstance("new_name", URI);
    service.register(federatedMetaStore);
    verify(federatedMetaStoreStorage).insert(federatedMetaStore);
    verify(federationEventListener).onRegister(federatedMetaStore);
  }

  @Test(expected = ValidationException.class)
  public void registerAnExistingDatabasePrefix() {
    AbstractMetaStore federatedMetaStore = newFederatedInstance(METASTORE_NAME, URI);
    service.register(federatedMetaStore);
  }

  @Test
  public void updateAnExistingDatabasePrefix() {
    AbstractMetaStore old = newFederatedInstance(METASTORE_NAME, URI);
    AbstractMetaStore newMetastore = newFederatedInstance(METASTORE_NAME, URI + "NEW");
    service.update(old, newMetastore);
    verify(federatedMetaStoreStorage).update(old, newMetastore);
    verify(federationEventListener).onUpdate(old, newMetastore);
  }

  @Test
  public void updateAnExistingDatabasePrefixToNewPrefix() {
    AbstractMetaStore old = newFederatedInstance(METASTORE_NAME, URI);
    AbstractMetaStore newMetastore = newFederatedInstance(METASTORE_NAME + "new", URI + "NEW");
    service.update(old, newMetastore);
    verify(federatedMetaStoreStorage).update(old, newMetastore);
    verify(federationEventListener).onUpdate(old, newMetastore);
  }

  @Test(expected = ValidationException.class)
  public void updateAnNonRegisteredMetastoreFails() {
    AbstractMetaStore federatedMetaStore = newFederatedInstance(METASTORE_NAME + "new", URI);
    service.update(federatedMetaStore, federatedMetaStore);
  }

  @Test(expected = ValidationException.class)
  public void updateToExistingMetastoreFails() {
    AbstractMetaStore oldMetaStore = newFederatedInstance(METASTORE_NAME, URI);
    AbstractMetaStore newMetaStore = newFederatedInstance(METASTORE_NAME + "new", URI);
    when(federatedMetaStoreStorage.get(newMetaStore.getName())).thenReturn(newMetaStore);

    service.update(oldMetaStore, newMetaStore);
  }

  @Test
  public void unregister() {
    service.unregister(METASTORE_NAME);
    verify(federatedMetaStoreStorage).delete(METASTORE_NAME);
    verify(federationEventListener).onUnregister(newFederatedInstance(METASTORE_NAME, URI));
  }

  @Test(expected = ValidationException.class)
  public void unregisterANonExistentMetastore() {
    String name = "new_name";
    service.unregister(name);
  }

  @Test
  public void get() {
    AbstractMetaStore federatedMetaStores = service.get(METASTORE_NAME);
    verify(federatedMetaStoreStorage).get(METASTORE_NAME);
    assertThat(federatedMetaStores.getName(), is(METASTORE_NAME));
  }

  @Test
  public void getAll() {
    List<AbstractMetaStore> federatedMetaStores = service.getAll();
    verify(federatedMetaStoreStorage).getAll();
    assertThat(federatedMetaStores.size(), is(1));
    assertThat(federatedMetaStores.get(0).getName(), is(METASTORE_NAME));
  }

}
