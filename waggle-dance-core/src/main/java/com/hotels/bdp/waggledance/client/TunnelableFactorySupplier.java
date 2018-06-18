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

import com.hotels.hcommon.hive.metastore.client.api.CloseableIFace;
import com.hotels.hcommon.ssh.SshSettings;
import com.hotels.hcommon.ssh.TunnelableFactory;

public class TunnelableFactorySupplier {

  private static final String YES = "yes";
  private static final String NO = "no";

  /*
   * Note at the moment this class will create a factory each time the method is invoked. This means that each new
   * client will have its own exclusive SSH tunnel. We could cache the factory instead and use one tunnel for each
   * HiveConf, i.e. federated metastore configuration, and share the tunnel for all its clients. We must evaluate what's
   * more convenient/efficient in Waggle Dance.
   */
  public TunnelableFactory<CloseableIFace> get(HiveConf hiveConf) {
    String strictHostKeyCheckingString = hiveConf.get(WaggleDanceHiveConfVars.SSH_STRICT_HOST_KEY_CHECKING.varname);
    if (!(YES.equals(strictHostKeyCheckingString) || NO.equals(strictHostKeyCheckingString))) {
      throw new IllegalArgumentException("Invalid strict host key checking: must be either 'yes' or 'no'");
    }
    boolean strictHostKeyChecking = YES.equals(strictHostKeyCheckingString) ? true : false;
    SshSettings sshSettings = SshSettings
        .builder()
        .withSshPort(hiveConf.getInt(WaggleDanceHiveConfVars.SSH_PORT.varname, SshSettings.DEFAULT_SSH_PORT))
        .withSessionTimeout(
            hiveConf.getInt(WaggleDanceHiveConfVars.SSH_SESSION_TIMEOUT.varname, SshSettings.DEFAULT_SESSION_TIMEOUT))
        .withRoute(hiveConf.get(WaggleDanceHiveConfVars.SSH_ROUTE.varname))
        .withKnownHosts(hiveConf.get(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname))
        .withPrivateKeys(hiveConf.get(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname))
        .withStrictHostKeyChecking(strictHostKeyChecking)
        .build();
    return new TunnelableFactory<>(sshSettings);

  }

}
