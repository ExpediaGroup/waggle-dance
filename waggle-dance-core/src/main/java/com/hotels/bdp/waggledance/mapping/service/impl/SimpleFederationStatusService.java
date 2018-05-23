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

import org.springframework.stereotype.Service;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.bdp.waggledance.api.federation.service.FederationStatusService;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;
import com.hotels.bdp.waggledance.metastore.CloseableThriftHiveMetaStoreClientFactory;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;

@Service
public class SimpleFederationStatusService implements FederationStatusService {

  /**
   * Checks the status of an {@code AbstractMetaStore}.
   * <p>
   * Note: we expect this service should have a low usage so creating a client on each request is fine. If the usage
   * increases impacting the performance of the server then caching or an alternative solution could be adopted.
   * </p>
   *
   * @param metaStoreUris URI of the metastore to check.
   * @return {@code MetaStoreStatus.AVAILABLE} if the service can successfully connect to the metastore. Otherwise,
   * returns {@code MetaStoreStatus.UNAVAILABLE}
   */
  @Override
  public MetaStoreStatus checkStatus(AbstractMetaStore metaStore) {
    try (CloseableMetaStoreClient client = getMetaStoreClientFactory(metaStore).newInstance()) {
      if (!client.isOpen()) {
        return MetaStoreStatus.UNAVAILABLE;
      }
    } catch (Exception e) {
      return MetaStoreStatus.UNAVAILABLE;
    }
    return MetaStoreStatus.AVAILABLE;
  }

  @VisibleForTesting
  public MetaStoreClientFactory getMetaStoreClientFactory(AbstractMetaStore metaStore) {
    return new CloseableThriftHiveMetaStoreClientFactory(metaStore);
  }

}
