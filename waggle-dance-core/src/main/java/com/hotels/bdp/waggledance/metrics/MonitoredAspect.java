/**
 * Copyright (C) 2016-2018 Expedia Inc.
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

import java.util.concurrent.TimeUnit;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;

import io.micrometer.graphite.GraphiteMeterRegistry;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Stopwatch;

@Aspect
@Configurable
public class MonitoredAspect {

  private static final String TIMER = "timer";
  private static final String COUNTER = "counter";
  private static final Joiner DOT_JOINER = Joiner.on(".");

  private @Autowired GraphiteMeterRegistry graphiteMeterRegistry;
  // private @Autowired CounterService counterService;
  // private @Autowired GaugeService gaugeService;

  @Around("execution(public * *(..)) && within(@com.hotels.bdp.waggledance.metrics.Monitored *)")
  public Object monitor(ProceedingJoinPoint pjp) throws Throwable {
    return monitor(pjp, null);
  }

  @Around("@annotation(monitored)")
  public Object monitor(ProceedingJoinPoint pjp, Monitored monitored) throws Throwable {

    String metricBasePath = buildMetricBasePath(pjp);

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
      increment(buildMetricPath(COUNTER, metricBasePath, getMonitorMetastore(), "calls"));
      increment(buildMetricPath(COUNTER, metricBasePath, getMonitorMetastore(), result));
      submit(buildMetricPath(TIMER, metricBasePath, getMonitorMetastore(), "duration"),
          stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
  }

  @VisibleForTesting
  void setMeterRegistry(GraphiteMeterRegistry graphiteMeterRegistry) {
    this.graphiteMeterRegistry = graphiteMeterRegistry;
  }

  private void increment(String metricName) {
    if (graphiteMeterRegistry != null) {
      // Metrics.counter(metricName).increment();
      graphiteMeterRegistry.counter(metricName).increment();

    }
  }

  private void submit(String metricName, long value) {
    if (graphiteMeterRegistry != null) {
      // Metrics.gauge(metricName, value);
      graphiteMeterRegistry.gauge(metricName, value);
      // gaugeService.submit(metricName, value);
    }
  }

  private String buildMetricBasePath(ProceedingJoinPoint pjp) {
    String className = clean(pjp.getSignature().getDeclaringTypeName());
    String methodName = clean(pjp.getSignature().getName());
    return new StringBuilder(className).append(".").append(methodName).toString();
  }

  private String clean(String string) {
    String result = string.replaceAll("\\$|<|>", "_");
    return result;
  }

  private String buildMetricPath(String... parts) {
    return DOT_JOINER.join(parts);
  }

}
