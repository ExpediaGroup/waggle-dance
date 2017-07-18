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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

import java.util.Arrays;

import org.apache.hadoop.hive.conf.HiveConf;

public class SessionFactorySupplierFactory {

  public SessionFactorySupplier newInstance(HiveConf hiveConf) {
    int sshPort = hiveConf.getInt(WaggleDanceHiveConfVars.SSH_PORT.varname, 22);
    String knownHosts = hiveConf.get(WaggleDanceHiveConfVars.SSH_KNOWN_HOSTS.varname);
    String privateKeys = hiveConf.get(WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname);

    checkArgument(sshPort > 0 && sshPort <= 65536,
        WaggleDanceHiveConfVars.SSH_PORT.varname + " must be a number between 1 and 65536");
    checkArgument(!isNullOrEmpty(privateKeys), WaggleDanceHiveConfVars.SSH_PRIVATE_KEYS.varname + " cannot be null");

    SessionFactorySupplier sessionFactorySupplier = new SessionFactorySupplier(sshPort, knownHosts,
        Arrays.asList(privateKeys.split(",")));
    return sessionFactorySupplier;
  }

}
