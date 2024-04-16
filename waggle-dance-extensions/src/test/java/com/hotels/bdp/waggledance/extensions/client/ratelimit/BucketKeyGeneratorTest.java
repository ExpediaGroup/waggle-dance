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
