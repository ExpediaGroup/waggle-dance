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
package com.hotels.bdp.waggledance.server;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.model.DatabaseResolution;
import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;
import com.hotels.bdp.waggledance.mapping.model.QueryMapping;
import com.hotels.bdp.waggledance.mapping.service.MetaStoreMappingFactory;
import com.hotels.bdp.waggledance.mapping.service.impl.NotifyingFederationService;

@RunWith(MockitoJUnitRunner.class)
public class FederatedHMSHandlerFactoryTest {

  private final HiveConf hiveConf = new HiveConf();
  private @Mock WaggleDanceConfiguration waggleDanceConfiguration;
  private @Mock NotifyingFederationService notifyingFederationService;
  private @Mock MetaStoreMappingFactory metaStoreMappingFactory;
  private @Mock QueryMapping queryMapping;
  private @Mock SaslServerWrapper saslServerWrapper;
  private FederatedHMSHandlerFactory factory;

  @Before
  public void init() {
    when(notifyingFederationService.getAll()).thenReturn(new ArrayList<>());
    factory = new FederatedHMSHandlerFactory(hiveConf, notifyingFederationService, metaStoreMappingFactory,
        waggleDanceConfiguration, queryMapping, saslServerWrapper);
  }

  @Test
  public void typical() throws Exception {
    when(waggleDanceConfiguration.getDatabaseResolution()).thenReturn(DatabaseResolution.MANUAL);
    CloseableIHMSHandler handler = factory.create();
    assertThat(handler, is(instanceOf(FederatedHMSHandler.class)));
  }

  @Test
  public void prefixedDatabase() throws Exception {
    when(waggleDanceConfiguration.getDatabaseResolution()).thenReturn(DatabaseResolution.PREFIXED);
    factory = new FederatedHMSHandlerFactory(hiveConf, notifyingFederationService, metaStoreMappingFactory,
        waggleDanceConfiguration, queryMapping, saslServerWrapper);
    CloseableIHMSHandler handler = factory.create();
    assertThat(handler, is(instanceOf(FederatedHMSHandler.class)));
  }

  @Test(expected = WaggleDanceException.class)
  public void noMode() {
    factory = new FederatedHMSHandlerFactory(hiveConf, notifyingFederationService, metaStoreMappingFactory,
        waggleDanceConfiguration, queryMapping, saslServerWrapper);
    factory.create();
  }

}
