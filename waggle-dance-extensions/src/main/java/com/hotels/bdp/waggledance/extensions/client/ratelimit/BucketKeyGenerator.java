package com.hotels.bdp.waggledance.extensions.client.ratelimit;

public class BucketKeyGenerator {

  private final String prefix;

  public BucketKeyGenerator(String prefix) {
    this.prefix = prefix;
  }

  public String generateKey(String key) {
    if (prefix != null && !prefix.isEmpty()) {
      return prefix + "_" + key;
    }
    return key;
  }

}
