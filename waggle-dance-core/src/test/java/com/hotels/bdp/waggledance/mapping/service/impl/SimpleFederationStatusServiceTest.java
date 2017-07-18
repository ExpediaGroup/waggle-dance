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
package com.hotels.bdp.waggledance.mapping.service.impl;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Rule;
import org.junit.Test;

import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;
import com.hotels.beeju.ThriftHiveMetaStoreJUnitRule;

public class SimpleFederationStatusServiceTest {

  public @Rule ThriftHiveMetaStoreJUnitRule metastoreService = new ThriftHiveMetaStoreJUnitRule();

  private SimpleFederationStatusService service = new SimpleFederationStatusService();

  @Test
  public void metastoreIsAvailable() {
    assertThat(service.checkStatus(metastoreService.getThriftConnectionUri()), is(MetaStoreStatus.AVAILABLE));
  }

  @Test
  public void metastoreIsNotAvailable() {
    assertThat(service.checkStatus("thrift://unknown:9083"), is(MetaStoreStatus.UNAVAILABLE));
  }

}
