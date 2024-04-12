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
package com.hotels.bdp.waggledance.extensions.client.ratelimit.memory;

import java.util.HashMap;
import java.util.Map;

import io.github.bucket4j.Bucket;

import com.hotels.bdp.waggledance.extensions.client.ratelimit.BucketBandwithProvider;
import com.hotels.bdp.waggledance.extensions.client.ratelimit.BucketService;

/**
 * This class is mostly intended for testing or if you want to have a simple in-memory rate limiter and there is just
 * one Waggle Dance instance deployed. 
 */
public class InMemoryBucketService implements BucketService {

  private final BucketBandwithProvider bucketBandwithProvider;
  private Map<String, Bucket> bucketsPerUser = new HashMap<>();

  public InMemoryBucketService(BucketBandwithProvider bucketBandwithProvider) {
    this.bucketBandwithProvider = bucketBandwithProvider;
  }

  private Bucket createNewBucket() {
    return Bucket.builder().addLimit(bucketBandwithProvider.getBandwith()).build();
  }

  public Bucket getBucket(String key) {
    Bucket bucket = bucketsPerUser.get(key);
    if (bucket == null) {
      bucket = createNewBucket();
      bucketsPerUser.put(key, bucket);
    }
    return bucket;
  }

}
