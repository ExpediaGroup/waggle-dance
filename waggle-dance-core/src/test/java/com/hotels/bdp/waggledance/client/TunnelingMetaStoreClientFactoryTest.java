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

    when(builder.withHiveConf(any(HiveConf.class))).thenReturn(builder);
    when(builder.withLocalHost(anyString())).thenReturn(builder);
    when(builder.withName(anyString())).thenReturn(builder);
    when(builder.withReconnectionRetries(anyInt())).thenReturn(builder);
    when(builder.withRemoteHost(anyString())).thenReturn(builder);
    when(builder.withRemotePort(anyInt())).thenReturn(builder);
    when(builder.withSSHRoute(anyString())).thenReturn(builder);
    when(builder.withTunnelConnectionManagerFactory(any(TunnelConnectionManagerFactory.class))).thenReturn(builder);
    when(builder.build()).thenReturn(client);

    factory.newInstance(hiveConf, "test", 10);

    verify(builder).withRemotePort(anyInt());
    verify(builder).withLocalHost(anyString());
    verify(builder).withSSHRoute(anyString());
    verify(builder).withRemoteHost(anyString());
    verify(builder).withName(anyString());
    verify(builder).withReconnectionRetries(anyInt());
    verify(builder).withHiveConf(any(HiveConf.class));
    verify(builder).withTunnelConnectionManagerFactory(any(TunnelConnectionManagerFactory.class));
    verify(builder).build();
  }

  @Test
  public void newInstanceWithoutTunneling() throws Exception {
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS,
        "thrift://internal-test-shared-hive-metastore-elb-1234567891.us-west-1.elb.amazonaws.com:9083");
    TunnelingMetaStoreClientFactory factory = new TunnelingMetaStoreClientFactory(new SessionFactorySupplierFactory(),
        builder);

    factory.newInstance(hiveConf, "test", 10);

    verify(builder, times(0)).withRemotePort(anyInt());
    verify(builder, times(0)).withLocalHost(anyString());
    verify(builder, times(0)).withSSHRoute(anyString());
    verify(builder, times(0)).withRemoteHost(anyString());
    verify(builder, times(0)).withName(anyString());
    verify(builder, times(0)).withReconnectionRetries(anyInt());
    verify(builder, times(0)).withHiveConf(any(HiveConf.class));
    verify(builder, times(0)).withTunnelConnectionManagerFactory(any(TunnelConnectionManagerFactory.class));
    verify(builder, times(0)).build();
  }
}