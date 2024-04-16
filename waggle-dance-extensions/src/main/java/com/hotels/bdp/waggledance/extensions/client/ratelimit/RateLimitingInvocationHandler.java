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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Set;

import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.bucket4j.Bucket;
import io.github.bucket4j.ConsumptionProbe;

import com.google.common.collect.Sets;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.bdp.waggledance.server.WaggleDanceServerException;

class RateLimitingInvocationHandler implements InvocationHandler {
  private static Logger log = LoggerFactory.getLogger(RateLimitingInvocationHandler.class);
  
  static final String UNKNOWN_USER = "_UNKNOWN_USER_";
  private static final Set<String> IGNORABLE_METHODS = Sets.newHashSet("isOpen", "close", "set_ugi", "flushCache");
  private String metastoreName;
  private CloseableThriftHiveMetastoreIface client;
  private String user = UNKNOWN_USER;

  private final BucketService bucketService;
  private final BucketKeyGenerator bucketKeyGenerator;

  public RateLimitingInvocationHandler(
      CloseableThriftHiveMetastoreIface client,
      String metastoreName,
      BucketService bucketService, BucketKeyGenerator bucketKeyGenerator) {
    this.client = client;
    this.metastoreName = metastoreName;
    this.bucketService = bucketService;
    this.bucketKeyGenerator = bucketKeyGenerator;
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    if (method.getName().equals("set_ugi")) {
      user = (String) args[0];
    }
    if (isIgnoredMethod(method.getName())) {
      return doRealCall(client, method, args);
    } else {
      return doRateLimitCall(client, method, args);
    }
  }

  private Object doRateLimitCall(CloseableThriftHiveMetastoreIface client, Method method, Object[] args)
    throws IllegalAccessException, Throwable {
    if (shouldProceedWithCall(method)) {
      return doRealCall(client, method, args);
    } else {
      log.info("User '{}' made too many requests.", user);
      // HTTP status would be 429, so using same for Thrift.
      throw new WaggleDanceServerException("[STATUS=429] Too many requests.");
    }
  }

  private boolean shouldProceedWithCall(Method method) {
    try {
      Bucket bucket = bucketService.getBucket(bucketKeyGenerator.generateKey(user));
      ConsumptionProbe probe = bucket.tryConsumeAndReturnRemaining(1);
      log
          .info("RateLimitCall:[User:{}, method:{}, source_ip:{}, tokens_remaining:{}, metastoreName:{}]", user,
              method.getName(), HMSHandler.getThreadLocalIpAddress(), probe.getRemainingTokens(), metastoreName);
      return probe.isConsumed();
    } catch (Exception e) {
      if (log.isDebugEnabled()) {
        log.error("Error while processing rate limit for: User:{}, method:{}", user, method.getName(), e);
      } else {
        log
            .error("Error while processing rate limit for: User:{}, method:{}, message:{}", user, method.getName(),
                e.getMessage());
      }
      return true;
    }
  }

  private Object doRealCall(CloseableThriftHiveMetastoreIface client, Method method, Object[] args)
    throws IllegalAccessException, Throwable {
    try {
      return method.invoke(client, args);
    } catch (InvocationTargetException e) {
      Throwable realException = e.getTargetException();
      throw realException;
    }
  }

  /**
   * Ignore some methods that are not "real" metastore calls or should not count towards a rate limit.
   * 
   * @param method
   * @return true if the method should be ignored for rate limiting purposes.
   */
  private boolean isIgnoredMethod(String methodName) {
    return IGNORABLE_METHODS.contains(methodName);
  }
}
