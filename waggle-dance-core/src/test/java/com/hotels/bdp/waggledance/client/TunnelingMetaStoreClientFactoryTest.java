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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TunnelingMetaStoreClientFactoryTest {

  private @Mock TunnelingMetastoreClientBuilder builder;
  private @Mock CloseableThriftHiveMetastoreIface client;
  private @Spy HiveConf hiveConf = new HiveConf();

  @Test
  public void newInstanceWithTunneling() throws Exception {
    TunnelingMetaStoreClientFactory factory = new TunnelingMetaStoreClientFactory(new SessionFactorySupplierFactory(),
        builder);

    hiveConf.set(WaggleDanceHiveConfVars.SSH_ROUTE.varname, "hcom@ec2-12-345-678-91.compute-1.amazonaws.com");
    hiveConf.set(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname, "private_key");
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,
        "thrift://internal-test-shared-hive-metastore-elb-1234567891.us-west-1.elb.amazonaws.com:9083");

    when(builder.setHiveConf(any(HiveConf.class))).thenReturn(builder);
    when(builder.setLocalHost(anyString())).thenReturn(builder);
    when(builder.setName(anyString())).thenReturn(builder);
    when(builder.setReconnectionRetries(anyInt())).thenReturn(builder);
    when(builder.setRemoteHost(anyString())).thenReturn(builder);
    when(builder.setRemotePort(anyInt())).thenReturn(builder);
    when(builder.setSSHRoute(anyString())).thenReturn(builder);
    when(builder.setTunnelConnectionManagerFactory(any(TunnelConnectionManagerFactory.class))).thenReturn(builder);
    when(builder.build()).thenReturn(client);

    factory.newInstance(hiveConf, "test", 10);

    verify(builder).setRemotePort(anyInt());
    verify(builder).setLocalHost(anyString());
    verify(builder).setSSHRoute(anyString());
    verify(builder).setRemoteHost(anyString());
    verify(builder).setName(anyString());
    verify(builder).setReconnectionRetries(anyInt());
    verify(builder).setHiveConf(any(HiveConf.class));
    verify(builder).setTunnelConnectionManagerFactory(any(TunnelConnectionManagerFactory.class));
    verify(builder).build();
  }

  @Test
  public void newInstanceWithoutTunneling() throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,
        "thrift://internal-test-shared-hive-metastore-elb-1234567891.us-west-1.elb.amazonaws.com:9083");
    TunnelingMetaStoreClientFactory factory = new TunnelingMetaStoreClientFactory(new SessionFactorySupplierFactory(),
        builder);

    factory.newInstance(hiveConf, "test", 10);

    verify(builder, times(0)).setRemotePort(anyInt());
    verify(builder, times(0)).setLocalHost(anyString());
    verify(builder, times(0)).setSSHRoute(anyString());
    verify(builder, times(0)).setRemoteHost(anyString());
    verify(builder, times(0)).setName(anyString());
    verify(builder, times(0)).setReconnectionRetries(anyInt());
    verify(builder, times(0)).setHiveConf(any(HiveConf.class));
    verify(builder, times(0)).setTunnelConnectionManagerFactory(any(TunnelConnectionManagerFactory.class));
    verify(builder, times(0)).build();
  }
}