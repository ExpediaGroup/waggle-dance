/**
 * Copyright (C) 2016-2017 Expedia Inc.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.hotels.bdp.waggledance.api.federation.service.FederationStatusService;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.client.pool.MetaStoreClientPool;

@Service
public class ConnectionRecyclingFederationStatusService implements FederationStatusService {

  private final static Logger log = LoggerFactory.getLogger(ConnectionRecyclingFederationStatusService.class);
  private final MetaStoreClientPool metaStoreClientPool;

  @Autowired
  public ConnectionRecyclingFederationStatusService(MetaStoreClientPool metaStoreClientPool) {
    this.metaStoreClientPool = metaStoreClientPool;
  }

  /**
   * Checks the status of an {@code AbstractMetaStore}. Tries to reuse any existing client that was created.
   *
   * @param metastore to check.
   * @return {@code MetaStoreStatus.AVAILABLE} if the service can successfully connect to the metastore. Otherwise,
   *         returns {@code MetaStoreStatus.UNAVAILABLE}
   */
  @Override
  public MetaStoreStatus checkStatus(AbstractMetaStore metaStore) {
    CloseableThriftHiveMetastoreIface client = null;
    try {
      client = metaStoreClientPool.borrowObjectUnchecked(metaStore);
      if (client.isOpen()) {
        log.debug("Status check for {} is AVAILABLE", metaStore);
        return MetaStoreStatus.AVAILABLE;
      } else {
        log.debug("Status check for {} is UNAVAILABLE", metaStore);
        return MetaStoreStatus.UNAVAILABLE;
      }
    } catch (Exception e) {
      log.debug("Status check for {} is UNAVAILABLE", metaStore);
      return MetaStoreStatus.UNAVAILABLE;
    } finally {
      metaStoreClientPool.returnObjectUnchecked(metaStore, client);
    }
  }

}
