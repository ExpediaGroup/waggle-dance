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
package com.hotels.bdp.waggledance.client;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;

public class TunnelableFactorySupplierTest {

  private final HiveConf hiveConf = new HiveConf();
  private final TunnelableFactorySupplier supplier = new TunnelableFactorySupplier();

  @Before
  public void init() {
    hiveConf.set(WaggleDanceHiveConfVars.SSH_ROUTE.varname, "user@hop1 -> hop2");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, "private_key");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname, "known_hosts");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_STRICT_HOST_KEY_CHECKING.varname, "yes");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://metastore:1234");
  }

  @Test
  public void typical() throws Exception {
    supplier.get(hiveConf);
  }

  @Test
  public void highPort() {
    hiveConf.setInt(WaggleDanceHiveConfVars.SSH_PORT.varname, 65535);
    supplier.get(hiveConf);
  }

  @Test
  public void nullRoute() {
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_ROUTE.varname);
    supplier.get(hiveConf);
  }

  @Test
  public void emptyRoute() {
    hiveConf.set(WaggleDanceHiveConfVars.SSH_ROUTE.varname, "");
    supplier.get(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullKnownHosts() {
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname);
    supplier.get(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyKnownHosts() {
    hiveConf.set(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname, "");
    supplier.get(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void negativePort() {
    hiveConf.setInt(WaggleDanceHiveConfVars.SSH_PORT.varname, -1);
    supplier.get(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void zeroPort() {
    hiveConf.setInt(WaggleDanceHiveConfVars.SSH_PORT.varname, 0);
    supplier.get(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void tooHighPort() {
    hiveConf.setInt(WaggleDanceHiveConfVars.SSH_PORT.varname, 65537);
    supplier.get(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullPrivateKeys() {
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname);
    supplier.get(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyPrivateKeys() {
    hiveConf.set(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, "");
    supplier.get(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void nullStrictHostKeyCheckingSetting() {
    hiveConf.unset(WaggleDanceHiveConfVars.SSH_STRICT_HOST_KEY_CHECKING.varname);
    supplier.get(hiveConf);
  }

  @Test(expected = IllegalArgumentException.class)
  public void incorrectStrictHostKeyCheckingSetting() {
    hiveConf.set(WaggleDanceHiveConfVars.SSH_STRICT_HOST_KEY_CHECKING.varname, "foo");
    supplier.get(hiveConf);
  }

}
