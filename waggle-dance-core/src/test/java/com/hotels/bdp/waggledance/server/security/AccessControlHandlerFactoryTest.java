/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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
package com.hotels.bdp.waggledance.server.security;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.model.AccessControlType;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;

@RunWith(MockitoJUnitRunner.class)
public class AccessControlHandlerFactoryTest {

  @Mock
  private FederationService federationService;
  private AccessControlHandlerFactory factory;

  @Before
  public void setUp() {
    factory = new AccessControlHandlerFactory(federationService);
  }

  @Test
  public void newInstanceReadOnly() throws Exception {
    FederatedMetaStore federatedMetaStore = new FederatedMetaStore();
    federatedMetaStore.setAccessControlType(AccessControlType.READ_ONLY);
    AccessControlHandler newInstance = factory.newInstance(federatedMetaStore);
    assertTrue(newInstance instanceof ReadOnlyAccessControlHandler);
  }

  @Test
  public void newInstanceReadAndWriteOnDatabaseWhiteList() throws Exception {
    PrimaryMetaStore primaryMetaStore = new PrimaryMetaStore("primary", "",
        AccessControlType.READ_AND_WRITE_ON_DATABASE_WHITELIST);
    AccessControlHandler newInstance = factory.newInstance(primaryMetaStore);
    assertTrue(newInstance instanceof DatabaseWhitelistAccessControlHandler);
    assertFalse(newInstance.hasCreatePermission());
  }

  @Test
  public void newInstanceReadAndWriteAndCreate() throws Exception {
    PrimaryMetaStore primaryMetaStore = new PrimaryMetaStore("primary", "",
        AccessControlType.READ_AND_WRITE_AND_CREATE);
    AccessControlHandler newInstance = factory.newInstance(primaryMetaStore);
    assertTrue(newInstance instanceof ReadWriteCreateAccessControlHandler);
    assertTrue(newInstance.hasCreatePermission());
  }

  @Test(expected = IllegalStateException.class)
  public void newInstanceReadAndWriteAndCreateNotPrimary() {
    FederatedMetaStore federatedMetaStore = new FederatedMetaStore("federated", "",
        AccessControlType.READ_AND_WRITE_AND_CREATE);
    factory.newInstance(federatedMetaStore);
  }

  @Test
  public void newInstanceReadAndWriteAndCreateOnDatabaseWhiteList() throws Exception {
    PrimaryMetaStore primaryMetaStore = new PrimaryMetaStore("primary", "",
        AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST);
    AccessControlHandler newInstance = factory.newInstance(primaryMetaStore);
    assertTrue(newInstance instanceof DatabaseWhitelistAccessControlHandler);
    assertTrue(newInstance.hasCreatePermission());
  }

  @Test(expected = IllegalStateException.class)
  public void newInstanceReadAndWriteAndCreateOnDatabaseWhiteListNotPrimary() {
    FederatedMetaStore federatedMetaStore = new FederatedMetaStore("federated", "",
        AccessControlType.READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST);
    factory.newInstance(federatedMetaStore);
  }

}
