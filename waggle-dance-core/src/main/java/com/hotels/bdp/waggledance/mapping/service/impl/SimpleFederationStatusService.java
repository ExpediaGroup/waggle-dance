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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.hotels.bdp.waggledance.api.federation.service.FederationStatusService;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;

@Service
public class SimpleFederationStatusService implements FederationStatusService {

  private final static Logger log = LoggerFactory.getLogger(SimpleFederationStatusService.class);

  private HiveConf newHiveConf(String metaStoreUris) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.setVar(ConfVars.METASTOREURIS, metaStoreUris);
    return hiveConf;
  }

  /**
   * Checks the status of an {@code AbstractMetaStore}.
   * <p>
   * Note: we expect this service should have a low usage so creating a client on each request is fine. If the usage
   * increases impacting the performance of the server then caching or an alternative solution could be adopted.
   * </p>
   *
   * @param metaStoreUris URI of the metastore to check.
   * @return {@code MetaStoreStatus.AVAILABLE} if the service can successfully connect to the metastore. Otherwise,
   *         returns {@code MetaStoreStatus.UNAVAILABLE}
   */
  @Override
  public MetaStoreStatus checkStatus(String metaStoreUris) {
    HiveConf hiveConf = newHiveConf(metaStoreUris);
    HiveMetaStoreClient client = null;
    try {
      client = new HiveMetaStoreClient(hiveConf);
    } catch (Exception e) {
      log.debug("Status check for {} is UNAVAILABLE", metaStoreUris);
      return MetaStoreStatus.UNAVAILABLE;
    } finally {
      if (client != null) {
        client.close();
      }
    }
    log.debug("Status check for {} is AVAILABLE", metaStoreUris);
    return MetaStoreStatus.AVAILABLE;
  }

}
