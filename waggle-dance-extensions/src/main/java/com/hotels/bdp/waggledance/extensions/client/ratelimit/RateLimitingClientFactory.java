/**
 * Copyright (C) 2016-2024 Expedia, Inc.
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
package com.hotels.bdp.waggledance.extensions.client.ratelimit;

import java.lang.reflect.Proxy;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.client.ThriftClientFactory;

import io.micrometer.core.instrument.MeterRegistry;

public class RateLimitingClientFactory implements ThriftClientFactory {

  private static final Class<?>[] INTERFACES = new Class<?>[] { CloseableThriftHiveMetastoreIface.class };

  private final ThriftClientFactory thriftClientFactory;
  private final BucketService bucketService;
  private final BucketKeyGenerator bucketKeyGenerator;
  private final MeterRegistry meterRegistry;

  public RateLimitingClientFactory(
      ThriftClientFactory thriftClientFactory,
      BucketService bucketService,
      BucketKeyGenerator bucketKeyGenerator, MeterRegistry meterRegistry) {
    this.thriftClientFactory = thriftClientFactory;
    this.bucketService = bucketService;
    this.bucketKeyGenerator = bucketKeyGenerator;
    this.meterRegistry = meterRegistry;
  }

  @Override
  public CloseableThriftHiveMetastoreIface newInstance(AbstractMetaStore metaStore) {
    CloseableThriftHiveMetastoreIface client = thriftClientFactory.newInstance(metaStore);
    return (CloseableThriftHiveMetastoreIface) Proxy
        .newProxyInstance(getClass().getClassLoader(), INTERFACES,
            new RateLimitingInvocationHandler(client, metaStore.getName(), bucketService, bucketKeyGenerator, meterRegistry));

  }

}
