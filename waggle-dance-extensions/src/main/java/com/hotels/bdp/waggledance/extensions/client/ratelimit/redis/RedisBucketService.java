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
package com.hotels.bdp.waggledance.extensions.client.ratelimit.redis;

import io.github.bucket4j.Bucket;
import io.github.bucket4j.BucketConfiguration;
import io.github.bucket4j.redis.redisson.cas.RedissonBasedProxyManager;

import com.hotels.bdp.waggledance.extensions.client.ratelimit.BucketBandwidthProvider;
import com.hotels.bdp.waggledance.extensions.client.ratelimit.BucketService;

/**
 * This class uses a Redis server as a the storage back-end for the rate limiter. This is useful if you have multiple
 * Waggle Dance instances and you want to rate limit across all of them.
 */
public class RedisBucketService implements BucketService {

  private final RedissonBasedProxyManager<String> proxyManager;
  private final BucketConfiguration configuration;

  public RedisBucketService(
      BucketBandwidthProvider bucketBandwidthProvider,
      RedissonBasedProxyManager<String> proxyManager) {
    this.proxyManager = proxyManager;
    configuration = BucketConfiguration.builder().addLimit(bucketBandwidthProvider.getBandwidth()).build();
  }

  @Override
  public Bucket getBucket(String key) {
    Bucket bucket = proxyManager.builder().build(key, () -> configuration);
    return bucket;
  }

}
