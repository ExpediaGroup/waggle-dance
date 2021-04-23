/**
 * Copyright (C) 2016-2021 Expedia, Inc.
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
package com.hotels.bdp.waggledance.conf;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.google.common.collect.ImmutableMap;

@RunWith(SpringJUnit4ClassRunner.class)
@TestPropertySource(properties = { "port: 123", "verbose: true", "reserved-prefix: my_prefix_",
    "disconnect-connection-delay: 15", "disconnect-time-unit: seconds", "configuration-properties.prop1: val1" })
@ContextConfiguration(classes = { WaggleDanceConfiguration.class })
public class WaggleDanceConfigurationIntegrationTest {

  private @Autowired WaggleDanceConfiguration waggleDanceConfiguration;

  @Test
  public void typical() {
    assertThat(waggleDanceConfiguration.getPort(), is(123));
    assertThat(waggleDanceConfiguration.isVerbose(), is(true));
    assertThat(waggleDanceConfiguration.getDisconnectConnectionDelay(), is(15));
    assertThat(waggleDanceConfiguration.getDisconnectTimeUnit(), is(TimeUnit.SECONDS));

    Map<String, String> props = ImmutableMap.<String, String>builder().put("prop1", "val1").build();
    assertThat(waggleDanceConfiguration.getConfigurationProperties(), is(props));
  }

}
