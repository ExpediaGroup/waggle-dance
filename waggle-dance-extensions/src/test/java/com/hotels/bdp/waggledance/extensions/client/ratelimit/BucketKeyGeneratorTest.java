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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class BucketKeyGeneratorTest {

  @Test
    public void testGenerateKey() {
      BucketKeyGenerator bucketKeyGenerator = new BucketKeyGenerator("prefix");
      String key = bucketKeyGenerator.generateKey("key");
      assertThat(key, is("prefix_key"));
    }

  @Test
    public void testGenerateKeyNullPrefix() {
      BucketKeyGenerator bucketKeyGenerator = new BucketKeyGenerator(null);
      String key = bucketKeyGenerator.generateKey("key");
      assertThat(key, is("key"));
    }

  @Test
    public void testGenerateKeyEmptyPrefix() {
      BucketKeyGenerator bucketKeyGenerator = new BucketKeyGenerator("");
      String key = bucketKeyGenerator.generateKey("key");
      assertThat(key, is("key"));
    }

}
