/**
 * Copyright (C) 2016-2023 Expedia, Inc.
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
package com.hotels.bdp.waggledance.metrics;

import static com.hotels.bdp.waggledance.metrics.CurrentMonitoredMetaStoreHolder.getMonitorMetastore;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;

@Aspect
@Configurable
public class MonitoredAspect {

  private static final String TIMER = "timer";
  private static final String COUNTER = "counter";
  private static final Joiner DOT_JOINER = Joiner.on(".");

  private @Autowired MeterRegistry meterRegistry;

  @Around("execution(public * *(..)) && within(@com.hotels.bdp.waggledance.metrics.Monitored *)")
  public Object monitor(ProceedingJoinPoint pjp) throws Throwable {
    return monitor(pjp, null);
  }

  @Around("@annotation(monitored)")
  public Object monitor(ProceedingJoinPoint pjp, Monitored monitored) throws Throwable {

    String metricBasePath = buildMetricBasePath(pjp);
    String className = getClassName(pjp);
    String methodName = getMethodName(pjp);

    String result = null;
    Stopwatch stopwatch = Stopwatch.createStarted();
    try {
      Object returnObj = pjp.proceed();
      result = "success";
      return returnObj;
    } catch (Throwable t) {
      result = "failure";
      throw t;
    } finally {
      stopwatch.stop();
      increment(buildMetricName(COUNTER, metricBasePath, getMonitorMetastore(), "calls"),methodName,
          buildMetricName(COUNTER, className, "calls"));
      increment(buildMetricName(COUNTER, metricBasePath, getMonitorMetastore(), result),methodName,
          buildMetricName(COUNTER, className, result));
      submit(buildMetricName(TIMER, metricBasePath, getMonitorMetastore(), "duration"),
          stopwatch.elapsed(TimeUnit.MILLISECONDS),methodName, buildMetricName(TIMER, className, "duration"));
    }
  }

  @VisibleForTesting
  void setMeterRegistry(MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  private void increment(String metricName, String methodName, String metricWithTag) {
    if (meterRegistry != null) {
      Iterable<Tag> tags = getMetricsTags(methodName);
      meterRegistry.counter(metricName).increment();
      meterRegistry.counter(metricWithTag, tags).increment();
    }
  }

  private void submit(String metricName, long value, String methodName, String metricWithTag) {
    if (meterRegistry != null) {
      Iterable<Tag> tags = getMetricsTags(methodName);
      meterRegistry.timer(metricName).record(Duration.ofMillis(value));
      meterRegistry.timer(metricWithTag, tags).record(Duration.ofMillis(value));
    }
  }

  private Tags getMetricsTags(String methodName) {
    Tag federationTag = Tag.of("federation_namespace", getMonitorMetastore());
    Tag methodTag = Tag.of("method_name", methodName);
    return Tags.of(federationTag).and(methodTag);
  }

  private String getMethodName(ProceedingJoinPoint pjp) {
    return clean(pjp.getSignature().getName());
  }

  private String getClassName(ProceedingJoinPoint pjp) {
    return clean(pjp.getSignature().getDeclaringTypeName());
  }

  private String buildMetricBasePath(ProceedingJoinPoint pjp) {
    String className = getClassName(pjp);
    String methodName = getMethodName(pjp);
    return new StringBuilder(className).append(".").append(methodName).toString();
  }

  private String clean(String string) {
    String result = string.replaceAll("\\$|<|>", "_");
    return result;
  }

  private String buildMetricName(String... parts) {
    return DOT_JOINER.join(parts);
  }
}
