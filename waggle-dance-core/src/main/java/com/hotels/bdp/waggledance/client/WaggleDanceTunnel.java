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

import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jcraft.jsch.JSchException;
import com.pastdev.jsch.tunnel.TunnelConnectionManager;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

public class WaggleDanceTunnel {

  private static final Logger LOG = LoggerFactory.getLogger(WaggleDanceTunnel.class);

  private final HiveConf hiveConf;
  private final TunnelConnectionManager tunnelConnectionManager;
  private final String sshRoute;
  private final String localHost;
  private final String remoteHost;
  private final int remotePort;

  public WaggleDanceTunnel(
      HiveConf hiveConf,
      TunnelConnectionManager tunnelConnectionManager,
      String sshRoute,
      String localHost,
      String remoteHost,
      int remotePort) {
    this.hiveConf = hiveConf;
    this.tunnelConnectionManager = tunnelConnectionManager;
    this.sshRoute = sshRoute;
    this.localHost = localHost;
    this.remoteHost = remoteHost;
    this.remotePort = remotePort;
  }

  public HiveConf create() {
    try {
      LOG.debug("Creating tunnel: {}:? -> {} -> {}:{}", localHost, sshRoute, remoteHost, remotePort);
      int localPort = tunnelConnectionManager.getTunnel(remoteHost, remotePort).getAssignedLocalPort();
      tunnelConnectionManager.open();
      LOG.debug("Tunnel created: {}:{} -> {} -> {}:{}", localHost, localPort, sshRoute, remoteHost, remotePort);

      localPort = tunnelConnectionManager.getTunnel(remoteHost, remotePort).getAssignedLocalPort();
      HiveConf localHiveConf = new HiveConf(hiveConf);
      String proxyMetaStoreUris = "thrift://" + localHost + ":" + localPort;
      localHiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, proxyMetaStoreUris);
      LOG.info("Metastore URI {} is being proxied to {}", hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS),
          localHiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));
      return localHiveConf;
    } catch (JSchException | RuntimeException e) {
      String message = String.format("Unable to establish SSH tunnel: '%s:?' -> '%s' -> '%s:%s'", localHost, sshRoute,
          remoteHost, remotePort);
      throw new WaggleDanceException(message, e);
    }
  }
}
