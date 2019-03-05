/**
 * Copyright (C) 2016-2018 Expedia, Inc.
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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederationType;

@Component
public class AccessControlHandlerFactory {

  private static final boolean CAN_CREATE = true;
  private static final boolean CANNOT_CREATE = false;

  private final FederationService federationService;

  @Autowired
  public AccessControlHandlerFactory(FederationService federationService) {
    this.federationService = federationService;
  }

  public AccessControlHandler newInstance(AbstractMetaStore federatedMetaStore) {
    switch (federatedMetaStore.getAccessControlType()) {
    case READ_ONLY:
      return new ReadOnlyAccessControlHandler();
    case READ_AND_WRITE_ON_DATABASE_WHITELIST:
      return new DatabaseWhitelistAccessControlHandler(federatedMetaStore, federationService, CANNOT_CREATE);
    case READ_AND_WRITE_AND_CREATE:
      if (federatedMetaStore.getFederationType() == FederationType.PRIMARY) {
        return new ReadWriteCreateAccessControlHandler();
      } else {
        // Should never be possible to configure this state. If this is thrown it is a bug.
        throw new IllegalStateException("Write access on anything other then a 'primary' metastore is not allowed");
      }
    case READ_AND_WRITE_AND_CREATE_ON_DATABASE_WHITELIST:
      if (federatedMetaStore.getFederationType() == FederationType.PRIMARY) {
        return new DatabaseWhitelistAccessControlHandler(federatedMetaStore, federationService, CAN_CREATE);
      } else {
        // Should never be possible to configure this state. If this is thrown it is a bug.
        throw new IllegalStateException("Write access on anything other then a 'primary' metastore is not allowed");
      }

    default:
      throw new IllegalStateException("Cannot determine AcccessControlHandler type given type: '"
          + federatedMetaStore.getAccessControlType()
          + "'");
    }
  }
}
