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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import static com.hotels.bdp.waggledance.api.model.AbstractMetaStore.newFederatedInstance;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.hotels.bdp.waggledance.mapping.service.PrefixNamingStrategy;
import com.hotels.bdp.waggledance.mapping.service.impl.LowerCasePrefixNamingStrategy;
import com.hotels.bdp.waggledance.metrics.MonitoringConfiguration;
import com.hotels.bdp.waggledance.metrics.MonitoringConfigurationTestContext;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { CommonBeansTestContext.class, MonitoringConfiguration.class, MonitoringConfigurationTestContext.class, CommonBeans.class })
public class CommonBeansTest {

  private @Autowired HiveConf hiveConf;
  private @Autowired PrefixNamingStrategy namingStrategy;

  @Test
  public void hiveConf() {
    assertThat(hiveConf, is(notNullValue()));
    assertThat(hiveConf.get(CommonBeansTestContext.PROP_1), is(CommonBeansTestContext.VAL_1));
    assertThat(hiveConf.get(CommonBeansTestContext.PROP_2), is(CommonBeansTestContext.VAL_2));
  }

  @Test
  public void namingStrategy() {
    assertThat(namingStrategy, is(instanceOf(LowerCasePrefixNamingStrategy.class)));
    assertThat(namingStrategy.apply(newFederatedInstance("Name", null)), is("name_"));
  }

}
