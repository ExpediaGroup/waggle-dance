/**
 * Copyright (C) 2016-2017 Expedia Inc.
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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.boot.actuate.metrics.CounterService;
import org.springframework.boot.actuate.metrics.GaugeService;

@RunWith(MockitoJUnitRunner.class)
public class MonitoredAspectTest {

  private static final String MONITORED_TYPE = "Type$Anonymous";
  private static final String MONITORED_METHOD = "myMethod";

  private @Mock CounterService counterService;
  private @Mock GaugeService gaugeService;
  private @Mock ProceedingJoinPoint pjp;
  private @Mock Signature signature;
  private @Mock Monitored monitored;

  private MonitoredAspect aspect;

  @Before
  public void init() throws Exception {
    CurrentMonitoredMetaStoreHolder.monitorMetastore(null);
    when(signature.getDeclaringTypeName()).thenReturn(MONITORED_TYPE);
    when(signature.getName()).thenReturn(MONITORED_METHOD);
    when(pjp.getSignature()).thenReturn(signature);

    aspect = new MonitoredAspect();
    aspect.setCounterService(counterService);
    aspect.setGaugeService(gaugeService);
  }

  @Test
  public void specialChars() throws Throwable {
    reset(signature);
    when(signature.getDeclaringTypeName()).thenReturn("$Type<Enc>$");
    when(signature.getName()).thenReturn("<method$x>");

    aspect.monitor(pjp, monitored);

    ArgumentCaptor<String> metricCaptor = ArgumentCaptor.forClass(String.class);
    verify(counterService, times(2)).increment(metricCaptor.capture());
    assertThat(metricCaptor.getAllValues().get(0), is("counter._Type_Enc__._method_x_.all.calls"));
    assertThat(metricCaptor.getAllValues().get(1), is("counter._Type_Enc__._method_x_.all.success"));

    metricCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> durationCaptor = ArgumentCaptor.forClass(Long.class);
    verify(gaugeService).submit(metricCaptor.capture(), durationCaptor.capture());
    assertThat(metricCaptor.getValue(), is("timer._Type_Enc__._method_x_.all.duration"));
  }

  @Test
  public void monitorFailures() throws Throwable {
    when(pjp.proceed()).thenThrow(new ClassCastException());
    try {
      aspect.monitor(pjp, monitored);
    } catch (ClassCastException e) {
      // Expected
    }

    ArgumentCaptor<String> metricCaptor = ArgumentCaptor.forClass(String.class);
    verify(counterService, times(2)).increment(metricCaptor.capture());
    assertThat(metricCaptor.getAllValues().get(0), is("counter.Type_Anonymous.myMethod.all.calls"));
    assertThat(metricCaptor.getAllValues().get(1), is("counter.Type_Anonymous.myMethod.all.failure"));

    metricCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> durationCaptor = ArgumentCaptor.forClass(Long.class);
    verify(gaugeService).submit(metricCaptor.capture(), durationCaptor.capture());
    assertThat(metricCaptor.getValue(), is("timer.Type_Anonymous.myMethod.all.duration"));
  }

  @Test
  public void monitorSuccesses() throws Throwable {
    aspect.monitor(pjp, monitored);

    ArgumentCaptor<String> metricCaptor = ArgumentCaptor.forClass(String.class);
    verify(counterService, times(2)).increment(metricCaptor.capture());
    assertThat(metricCaptor.getAllValues().get(0), is("counter.Type_Anonymous.myMethod.all.calls"));
    assertThat(metricCaptor.getAllValues().get(1), is("counter.Type_Anonymous.myMethod.all.success"));

    metricCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> durationCaptor = ArgumentCaptor.forClass(Long.class);
    verify(gaugeService).submit(metricCaptor.capture(), durationCaptor.capture());
    assertThat(metricCaptor.getValue(), is("timer.Type_Anonymous.myMethod.all.duration"));
  }

  @Test
  public void monitorFailuresForSpecificMetastore() throws Throwable {
    CurrentMonitoredMetaStoreHolder.monitorMetastore("metastoreName");
    when(pjp.proceed()).thenThrow(new ClassCastException());
    try {
      aspect.monitor(pjp, monitored);
    } catch (ClassCastException e) {
      // Expected
    }

    ArgumentCaptor<String> metricCaptor = ArgumentCaptor.forClass(String.class);
    verify(counterService, times(2)).increment(metricCaptor.capture());
    assertThat(metricCaptor.getAllValues().get(0), is("counter.Type_Anonymous.myMethod.metastoreName.calls"));
    assertThat(metricCaptor.getAllValues().get(1), is("counter.Type_Anonymous.myMethod.metastoreName.failure"));

    metricCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> durationCaptor = ArgumentCaptor.forClass(Long.class);
    verify(gaugeService).submit(metricCaptor.capture(), durationCaptor.capture());
    assertThat(metricCaptor.getValue(), is("timer.Type_Anonymous.myMethod.metastoreName.duration"));
  }

  @Test
  public void monitorSuccessesForSpecificMetastore() throws Throwable {
    CurrentMonitoredMetaStoreHolder.monitorMetastore("metastoreName");
    aspect.monitor(pjp, monitored);

    ArgumentCaptor<String> metricCaptor = ArgumentCaptor.forClass(String.class);
    verify(counterService, times(2)).increment(metricCaptor.capture());
    assertThat(metricCaptor.getAllValues().get(0), is("counter.Type_Anonymous.myMethod.metastoreName.calls"));
    assertThat(metricCaptor.getAllValues().get(1), is("counter.Type_Anonymous.myMethod.metastoreName.success"));

    metricCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Long> durationCaptor = ArgumentCaptor.forClass(Long.class);
    verify(gaugeService).submit(metricCaptor.capture(), durationCaptor.capture());
    assertThat(metricCaptor.getValue(), is("timer.Type_Anonymous.myMethod.metastoreName.duration"));
  }

  @Test
  public void nullServices() throws Throwable {
    reset(signature);
    when(signature.getDeclaringTypeName()).thenReturn("Type");
    when(signature.getName()).thenReturn("method");

    aspect.setCounterService(null);
    aspect.setGaugeService(null);
    aspect.monitor(pjp, monitored);
  }

}
