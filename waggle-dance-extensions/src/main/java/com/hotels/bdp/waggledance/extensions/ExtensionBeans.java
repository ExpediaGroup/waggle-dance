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
package com.hotels.bdp.waggledance.extensions;

import java.io.IOException;
import java.time.Duration;

import org.redisson.Redisson;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.config.Config;
import org.redisson.connection.ConnectionManager;
import org.redisson.liveobject.core.RedissonObjectBuilder;
import org.redisson.reactive.CommandReactiveService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.github.bucket4j.distributed.ExpirationAfterWriteStrategy;
import io.github.bucket4j.distributed.serialization.Mapper;
import io.github.bucket4j.redis.redisson.cas.RedissonBasedProxyManager;

import com.hotels.bdp.waggledance.client.ThriftClientFactory;
import com.hotels.bdp.waggledance.extensions.client.ratelimit.BucketBandwithProvider;
import com.hotels.bdp.waggledance.extensions.client.ratelimit.BucketService;
import com.hotels.bdp.waggledance.extensions.client.ratelimit.RateLimitingClientFactory;
import com.hotels.bdp.waggledance.extensions.client.ratelimit.RefillType;
import com.hotels.bdp.waggledance.extensions.client.ratelimit.memory.InMemoryBucketService;
import com.hotels.bdp.waggledance.extensions.client.ratelimit.redis.RedisBucketService;

@Configuration
@ConditionalOnProperty(name = "waggledance.extensions.ratelimit.enabled", havingValue = "true")
public class ExtensionBeans {

  private static final String STORAGE_MEMORY = "MEMORY";
  private static final String STORAGE_REDIS= "REDIS";

  @Bean
  public ThriftClientFactory thriftClientFactory(
      ThriftClientFactory defaultWaggleDanceClientFactory,
      BucketService bucketService) {
    return new RateLimitingClientFactory(defaultWaggleDanceClientFactory, bucketService);
  }

  @ConditionalOnProperty(name = "waggledance.extensions.ratelimit.storage", havingValue = STORAGE_MEMORY)
  @Bean
  public BucketService inMemorybucketService(BucketBandwithProvider bucketBandwithProvider) {
    return new InMemoryBucketService(bucketBandwithProvider);
  }

  @ConditionalOnProperty(name = "waggledance.extensions.ratelimit.storage", havingValue = STORAGE_REDIS)
  @Bean
  public BucketService redisbucketService(
      BucketBandwithProvider bucketBandwithProvider,
      RedissonBasedProxyManager<String> redissonBasedProxyManager) {
    return new RedisBucketService(bucketBandwithProvider, redissonBasedProxyManager);
  }

  @ConditionalOnProperty(name = "waggledance.extensions.ratelimit.storage", havingValue = STORAGE_REDIS)
  @Bean
  public RedissonBasedProxyManager<String> redissonBasedProxyManager(
      @Value("${waggledance.extensions.ratelimit.reddison.embedded.config}") String embeddedConfigString)
    throws IOException {
    Config config = Config.fromYAML(embeddedConfigString);
    Redisson redisson = (Redisson) Redisson.create(config);
    ConnectionManager connectionManager = redisson.getConnectionManager();
    RedissonObjectBuilder objectBuilder = new RedissonObjectBuilder(redisson.reactive());
    CommandAsyncExecutor commandExecutor = new CommandReactiveService(connectionManager, objectBuilder);
    RedissonBasedProxyManager<String> proxyManager = RedissonBasedProxyManager
        .builderFor(commandExecutor)
        .withExpirationStrategy(ExpirationAfterWriteStrategy.basedOnTimeForRefillingBucketUpToMax(Duration.ofHours(24)))
        .withKeyMapper(Mapper.STRING)
        .build();
    return proxyManager;
  }

  @Bean
  public BucketBandwithProvider bucketBandwithProvider(
      @Value("${waggledance.extensions.ratelimit.capacity:2000}") long capacity,
      @Value("${waggledance.extensions.ratelimit.tokensPerMinute:1000}") long tokensPerMinute,
      @Value("${waggledance.extensions.ratelimit.refillType:GREEDY}") RefillType refillType) {
    return refillType.createBandwidthProvider(capacity, tokensPerMinute);
  }

}
