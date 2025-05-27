/**
 * Copyright (C) 2016-2025 Expedia, Inc.
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
package com.hotels.bdp.waggledance.context;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.doAnswer;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hotels.bdp.waggledance.mapping.service.impl.PollingFederationService;
import com.hotels.bdp.waggledance.metrics.MonitoringConfiguration;
import com.hotels.bdp.waggledance.metrics.MonitoringConfigurationTestContext;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = {
    MonitoringConfiguration.class,
    MonitoringConfigurationTestContext.class,
    ScheduledBeansTestContext.class,
    ScheduledBeans.class })
public class ScheduledBeansTest {

  private static Logger log = LoggerFactory.getLogger(ScheduledBeansTest.class);

  @Autowired
  private PollingFederationService pollingFederationService;

  @Test
  public void polling() {
    final AtomicInteger pollCallCount = new AtomicInteger(0);
    doAnswer((Answer<Void>) invocation -> {
      pollCallCount.incrementAndGet();
      log.info("test poll called");
      return null;
    }).when(pollingFederationService).poll();
    await().pollDelay(5, MILLISECONDS).atMost(500, MILLISECONDS).untilAtomic(pollCallCount, greaterThan(0));
  }

}
