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

import static org.junit.Assert.assertNotEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

import com.pastdev.jsch.tunnel.Tunnel;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

@RunWith(MockitoJUnitRunner.class)
public class TunnelConfigurationTest {

  private @Spy HiveConf hiveConf = new HiveConf();
  private @Mock TunnelConnectionManager tunnelConnectionManager;
  private @Mock Tunnel tunnel;

  @Test
  public void getConf() throws Exception {
    hiveConf.set(WaggleDanceHiveConfVars.SSH_ROUTE.varname, "hcom@ec2-12-345-678-91.compute-1.amazonaws.com");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, "private_key");
    String originalMetastoreUri = "thrift://internal-test-shared-hive-metastore-elb-1234567891.us-west-1.elb.amazonaws.com:9083";
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, originalMetastoreUri);

    when(tunnelConnectionManager.getTunnel(anyString(), anyInt())).thenReturn(tunnel);
    doNothing().when(tunnelConnectionManager).open();
    TunnelConfiguration tunnelConfiguration = new TunnelConfiguration(hiveConf, tunnelConnectionManager, "route",
        "localhost",
        "remotehost", 9083);
    HiveConf localHiveConf = tunnelConfiguration.getConf();
    assertNotEquals(originalMetastoreUri, localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
    assertNotEquals(hiveConf, localHiveConf);
    verify(tunnelConnectionManager, times(2)).getTunnel(anyString(), anyInt());
    verify(tunnelConnectionManager).open();
  }
}