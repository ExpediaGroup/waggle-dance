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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collection;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.cumulative.CumulativeCounter;
import io.micrometer.core.instrument.cumulative.CumulativeTimer;
import io.micrometer.core.instrument.search.RequiredSearch;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

@RunWith(MockitoJUnitRunner.class)
public class MonitoredAspectTest {

  private static final String MONITORED_TYPE = "Type$Anonymous";
  private static final String MONITORED_METHOD = "myMethod";

  private MeterRegistry meterRegistry;
  private @Mock ProceedingJoinPoint pjp;
  private @Mock Signature signature;
  private @Mock Monitored monitored;

  private MonitoredAspect aspect;

  @Before
  public void init() throws Exception {
    CurrentMonitoredMetaStoreHolder.monitorMetastore(null);

    meterRegistry = new SimpleMeterRegistry();

    when(signature.getDeclaringTypeName()).thenReturn(MONITORED_TYPE);
    when(signature.getName()).thenReturn(MONITORED_METHOD);
    when(pjp.getSignature()).thenReturn(signature);

    aspect = new MonitoredAspect();
    aspect.setMeterRegistry(meterRegistry);
  }

  @Test
  public void specialChars() throws Throwable {
    reset(signature);
    when(signature.getDeclaringTypeName()).thenReturn("$Type<Enc>$");
    when(signature.getName()).thenReturn("<method$x>");
    aspect.monitor(pjp, monitored);

    RequiredSearch rs = meterRegistry.get("counter._Type_Enc__._method_x_.all.calls");
    assertThat(rs.counter().count(), is(1.0));

    rs = meterRegistry.get("counter._Type_Enc__._method_x_.all.success");
    assertThat(rs.counter().count(), is(1.0));

    rs = meterRegistry.get("timer._Type_Enc__._method_x_.all.duration");
    assertThat(rs.timer().count(), is(1L));
  }

  @Test
  public void monitorFailures() throws Throwable {
    when(pjp.proceed()).thenThrow(new ClassCastException());
    try {
      aspect.monitor(pjp, monitored);
    } catch (ClassCastException e) {
      // Expected
    }

    RequiredSearch rs = meterRegistry.get("counter.Type_Anonymous.myMethod.all.calls");
    assertThat(rs.counter().count(), is(1.0));

    rs = meterRegistry.get("counter.Type_Anonymous.myMethod.all.failure");
    assertThat(rs.counter().count(), is(1.0));

    rs = meterRegistry.get("timer.Type_Anonymous.myMethod.all.duration");
    assertThat(rs.timer().count(), is(1L));
  }

  @Test
  public void monitorSuccesses() throws Throwable {
    aspect.monitor(pjp, monitored);

    RequiredSearch rs = meterRegistry.get("counter.Type_Anonymous.myMethod.all.calls");
    assertThat(rs.counter().count(), is(1.0));

    rs = meterRegistry.get("counter.Type_Anonymous.myMethod.all.success");
    assertThat(rs.counter().count(), is(1.0));

    rs = meterRegistry.get("timer.Type_Anonymous.myMethod.all.duration");
    assertThat(rs.timer().count(), is(1L));
  }

  @Test
  public void monitorSuccessWithTags() throws Throwable {
    aspect.monitor(pjp, monitored);

    Collection<Meter> successMeters = meterRegistry.get("counter.Type_Anonymous.success").meters();
    assertThat(successMeters.size(), is(1));
    assertThat(((CumulativeCounter) ((ArrayList) successMeters).get(0)).count(), is(1.0));

    Collection<Meter> callsMeters = meterRegistry.get("counter.Type_Anonymous.calls").meters();
    assertThat(callsMeters.size(), is(1));
    assertThat(((CumulativeCounter) ((ArrayList) callsMeters).get(0)).count(), is(1.0));

    Collection<Meter> durationMeters = meterRegistry.get("timer.Type_Anonymous.duration").meters();
    assertThat(durationMeters.size(), is(1));
    assertThat(((CumulativeTimer) ((ArrayList) durationMeters).get(0)).count(), is(1L));

    // Verify the tags for successMeters
    Meter successMeter = successMeters.iterator().next();
    assertThat(successMeter.getId().getTag("federation_namespace"), is("all"));
    assertThat(successMeter.getId().getTag("method_name"), is("myMethod"));

    // Verify the tags for callsMeters
    Meter callsMeter = callsMeters.iterator().next();
    assertThat(callsMeter.getId().getTag("federation_namespace"), is("all"));
    assertThat(successMeter.getId().getTag("method_name"), is("myMethod"));

    // Verify the tags for durationMeters
    Meter durationMeter = durationMeters.iterator().next();
    assertThat(durationMeter.getId().getTag("federation_namespace"), is("all"));
    assertThat(successMeter.getId().getTag("method_name"), is("myMethod"));
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

    RequiredSearch rs = meterRegistry.get("counter.Type_Anonymous.myMethod.metastoreName.calls");
    assertThat(rs.counter().count(), is(1.0));

    rs = meterRegistry.get("counter.Type_Anonymous.myMethod.metastoreName.failure");
    assertThat(rs.counter().count(), is(1.0));

    rs = meterRegistry.get("timer.Type_Anonymous.myMethod.metastoreName.duration");
    assertThat(rs.timer().count(), is(1L));
  }

  @Test
  public void monitorSuccessesForSpecificMetastore() throws Throwable {
    CurrentMonitoredMetaStoreHolder.monitorMetastore("metastoreName");
    aspect.monitor(pjp, monitored);

    RequiredSearch rs = meterRegistry.get("counter.Type_Anonymous.myMethod.metastoreName.calls");
    assertThat(rs.counter().count(), is(1.0));

    rs = meterRegistry.get("counter.Type_Anonymous.myMethod.metastoreName.success");
    assertThat(rs.counter().count(), is(1.0));

    rs = meterRegistry.get("timer.Type_Anonymous.myMethod.metastoreName.duration");
    assertThat(rs.timer().count(), is(1L));
  }

  @Test
  public void nullMeterRegistry() throws Throwable {
    reset(signature);
    when(signature.getDeclaringTypeName()).thenReturn("Type");
    when(signature.getName()).thenReturn("method");

    aspect.setMeterRegistry(null);
    aspect.monitor(pjp, monitored);
  }

}
