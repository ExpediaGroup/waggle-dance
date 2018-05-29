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

package com.hotels.bdp.waggledance.metastore;

import java.lang.reflect.Proxy;

import org.apache.hadoop.hive.conf.HiveConf;

import com.google.common.annotations.VisibleForTesting;

import com.hotels.hcommon.hive.metastore.MetaStoreClientException;
import com.hotels.hcommon.hive.metastore.client.api.CloseableMetaStoreClient;
import com.hotels.hcommon.hive.metastore.client.api.MetaStoreClientFactory;

public class ReconnectingMetaStoreClientFactory implements MetaStoreClientFactory {

  private static final Class<?>[] INTERFACES = new Class<?>[] { CloseableMetaStoreClient.class };

  private final int maxRetries;

  ReconnectingMetaStoreClientFactory(int maxRetries) {
    this.maxRetries = maxRetries;
  }

  public CloseableMetaStoreClient newInstance(HiveConf hiveConf, String name) {
    try {
      ReconnectingMetaStoreClientInvocationHandler reconnectingHandler = getReconectingMetaStoreClientInvocationHandler(
          hiveConf, name, maxRetries);
      return getProxyInstance(reconnectingHandler);
    } catch (Exception e) {
      String message = String.format("%s could not instantiate proxy with hive conf: %s. name: %s. max retries: %s",
          this.getClass().getName(), hiveConf, name, maxRetries);
      throw new MetaStoreClientException(message, e);
    }
  }

  @VisibleForTesting
  ReconnectingMetaStoreClientInvocationHandler getReconectingMetaStoreClientInvocationHandler(HiveConf hiveConf,
      String name, int maxRetries) {
    return new ReconnectingMetaStoreClientInvocationHandler(new ReconnectingThriftMetaStoreClient(hiveConf), name,
        maxRetries);
  }

  @VisibleForTesting
  CloseableMetaStoreClient getProxyInstance(ReconnectingMetaStoreClientInvocationHandler reconnectingHandler) {
    return (CloseableMetaStoreClient) Proxy.newProxyInstance(getClass().getClassLoader(), INTERFACES,
        reconnectingHandler);
  }

}
