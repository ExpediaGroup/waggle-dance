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

import java.time.Duration;

import io.github.bucket4j.Bandwidth;

public class GreedyBandwidthProvider implements BucketBandwidthProvider {

  private final long capacity;
  private final long tokensPerMinute;

  public GreedyBandwidthProvider(long capacity, long tokensPerMinute) {
    this.capacity = capacity;
    this.tokensPerMinute = tokensPerMinute;
  }

  @Override
  public Bandwidth getBandwidth() {
    Bandwidth limit = Bandwidth
        .builder()
        .capacity(capacity)
        .refillGreedy(tokensPerMinute, Duration.ofMinutes(1))
        .build();
    return limit;
  }

}
