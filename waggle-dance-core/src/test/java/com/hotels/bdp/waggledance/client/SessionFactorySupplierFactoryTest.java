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
package com.hotels.bdp.waggledance.client;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;

public class SessionFactorySupplierFactoryTest {

  private final SessionFactorySupplierFactory factory = new SessionFactorySupplierFactory();
  private final HiveConf hiveConf = new HiveConf();

  @Before
  public void init() {
    hiveConf.setInt(WaggleDanceHiveConfVars.SSH_PORT.varname, 2222);
    hiveConf.set(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname, "knownHosts");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, "key1,key2");
  }

  @Test
  public void typical() {
    SessionFactorySupplier supplier = factory.newInstance(hiveConf);
    assertThat(supplier, is(notNullValue()));
  }

  @Test
  public void highPort() {
    hiveConf.setInt(WaggleDanceHiveConfVars.SSH_PORT.varname, 65536);
    SessionFactorySupplier supplier = factory.newInstance(hiveConf);
    assertThat(supplier, is(notNullValue()));
  }

  @Test
  public void nullKnownHosts() {
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname);
    SessionFactorySupplier supplier = factory.newInstance(hiveConf);
    assertThat(supplier, is(notNullValue()));
  }

  @Test
  public void emptyKnownHosts() {
    hiveConf.set(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname, "");
    SessionFactorySupplier supplier = factory.newInstance(hiveConf);
    assertThat(supplier, is(notNullValue()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativePort() {
    hiveConf.setInt(WaggleDanceHiveConfVars.SSH_PORT.varname, -1);
    factory.newInstance(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroPort() {
    hiveConf.setInt(WaggleDanceHiveConfVars.SSH_PORT.varname, 0);
    factory.newInstance(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void tooHighPort() {
    hiveConf.setInt(WaggleDanceHiveConfVars.SSH_PORT.varname, 65537);
    factory.newInstance(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullKeys() {
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname);
    factory.newInstance(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyKeys() {
    hiveConf.set(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, "");
    factory.newInstance(hiveConf);
  }

}
