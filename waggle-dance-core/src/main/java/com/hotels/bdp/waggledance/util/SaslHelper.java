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
package com.hotels.bdp.waggledance.util;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hive.service.auth.HiveAuthConstants;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.auth.SaslQOP;
import org.apache.thrift.transport.TSaslServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SaslHelper {

  public static boolean isSASLWithKerberizedHadoop(HiveConf hiveconf) {
    return "kerberos".equalsIgnoreCase(hiveconf.get(HADOOP_SECURITY_AUTHENTICATION, "simple"))
            && !hiveconf.getVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION).equalsIgnoreCase(HiveAuthConstants.AuthTypes.NOSASL.getAuthName());
  }

  public static TTransportFactory getAuthTransFactory(HadoopThriftAuthBridge.Server saslServer, HiveConf hiveConf) throws LoginException {
    TTransportFactory transportFactory;
    TSaslServerTransport.Factory serverTransportFactory;
    String authTypeStr = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION);
    if (SaslHelper.isSASLWithKerberizedHadoop(hiveConf)) {
      try {
        serverTransportFactory = saslServer.createSaslServerTransportFactory(
                getSaslProperties(hiveConf));
      } catch (TTransportException e) {
        throw new LoginException(e.getMessage());
      }
      if (!authTypeStr.equalsIgnoreCase(HiveAuthConstants.AuthTypes.KERBEROS.getAuthName())) {
        throw new LoginException("Unsupported authentication type " + authTypeStr);
      }
      transportFactory = saslServer.wrapTransportFactory(serverTransportFactory);
    } else if (authTypeStr.equalsIgnoreCase(HiveAuthConstants.AuthTypes.NONE.getAuthName()) ||
            authTypeStr.equalsIgnoreCase(HiveAuthConstants.AuthTypes.LDAP.getAuthName()) ||
            authTypeStr.equalsIgnoreCase(HiveAuthConstants.AuthTypes.PAM.getAuthName()) ||
            authTypeStr.equalsIgnoreCase(HiveAuthConstants.AuthTypes.CUSTOM.getAuthName())) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else if (authTypeStr.equalsIgnoreCase(HiveAuthConstants.AuthTypes.NOSASL.getAuthName())) {
      transportFactory = new TTransportFactory();
    } else {
      throw new LoginException("Unsupported authentication type " + authTypeStr);
    }
    return transportFactory;
  }

  public static Map<String, String> getSaslProperties(HiveConf hiveConf) {
    Map<String, String> saslProps = new HashMap<String, String>();
    SaslQOP saslQOP = SaslQOP.fromString(hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP));
    saslProps.put(Sasl.QOP, saslQOP.toString());
    saslProps.put(Sasl.SERVER_AUTH, "true");
    return saslProps;
  }

}
