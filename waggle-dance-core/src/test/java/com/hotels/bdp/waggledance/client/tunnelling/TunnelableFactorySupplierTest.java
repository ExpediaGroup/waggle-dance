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
package com.hotels.bdp.waggledance.client.tunnelling;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;
import com.hotels.hcommon.hive.metastore.client.tunnelling.MetastoreTunnel;
import com.hotels.hcommon.ssh.SshSettings;
import com.hotels.hcommon.ssh.TunnelableFactory;

public class TunnelableFactorySupplierTest {

  private static final String TUNNEL_ROUTE = "user@hop1 -> hop2";
  private static final String TUNNEL_PRIVATE_KEY = "private_key";
  private static final String TUNNEL_KNOWN_HOSTS = "known_hosts";
  private static final String TUNNEL_STRICT_HOST_KEY_CHECKING = "yes";

  private final MetastoreTunnel metastoreTunnel = new MetastoreTunnel();
  private final TunnelableFactorySupplier supplier = new TunnelableFactorySupplier();

  @Before
  public void init() {
    metastoreTunnel.setRoute(TUNNEL_ROUTE);
    metastoreTunnel.setPrivateKeys(TUNNEL_PRIVATE_KEY);
    metastoreTunnel.setKnownHosts(TUNNEL_KNOWN_HOSTS);
    metastoreTunnel.setStrictHostKeyChecking(TUNNEL_STRICT_HOST_KEY_CHECKING);
  }

  @Test
  public void get() {
    TunnelableFactory<CloseableThriftHiveMetastoreIface> tunnelableFactory = supplier.get(metastoreTunnel);
    assertNotNull(tunnelableFactory);
  }

  @Test
  public void buildSshSettings() {
    SshSettings sshSettings = supplier.buildSshSettings(metastoreTunnel);
    assertThat(sshSettings.getRoute(), is(TUNNEL_ROUTE));
    assertThat(sshSettings.getPrivateKeys(), is(Lists.newArrayList(TUNNEL_PRIVATE_KEY)));
    assertThat(sshSettings.getKnownHosts(), is(TUNNEL_KNOWN_HOSTS));
    assertThat(sshSettings.isStrictHostKeyChecking(), is(true));
  }

}
