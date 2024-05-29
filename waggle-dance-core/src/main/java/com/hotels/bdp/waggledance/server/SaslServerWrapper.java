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
package com.hotels.bdp.waggledance.server;

import java.io.IOException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.security.DBTokenStore;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge.Server;
import org.apache.hadoop.hive.metastore.security.MetastoreDelegationTokenManager;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.transport.TTransportException;
import org.springframework.stereotype.Component;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import com.hotels.bdp.waggledance.util.SaslHelper;

@Component
@Log4j2
public class SaslServerWrapper {

  private MetastoreDelegationTokenManager delegationTokenManager;
  @Getter
  private static boolean useSasl;

  private HadoopThriftAuthBridge.Server saslServer = null;

  protected SaslServerWrapper(HiveConf conf)
      throws TTransportException {
    useSasl = conf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL);
    if (!useSasl) {
      return;
    }

    UserGroupInformation.setConfiguration(conf);

    if (SaslHelper.isSASLWithKerberizedHadoop(conf)) {
      saslServer =
          HadoopThriftAuthBridge.getBridge().createServer(
              conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB),
              conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL),
              conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_CLIENT_KERBEROS_PRINCIPAL));

      // Start delegation token manager
      delegationTokenManager = new MetastoreDelegationTokenManager();
      try {
        Object baseHandler = null;
        String tokenStoreClass = conf.getVar(
            HiveConf.ConfVars.METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_CLS);

        if (tokenStoreClass.equals(DBTokenStore.class.getName())) {
          // IMetaStoreClient is needed to access token store if DBTokenStore is to be used. It
          // will be got via Hive.get(conf).getMSC in a thread where the DelegationTokenStore
          // is called. To avoid the cyclic reference, we pass the Hive class to DBTokenStore where
          // it is used to get a threadLocal Hive object with a synchronized MetaStoreClient using
          // Java reflection.
          // Note: there will be two HS2 life-long opened MSCs, one is stored in HS2 thread local
          // Hive object, the other is in a daemon thread spawned in DelegationTokenSecretManager
          // to remove expired tokens.
          baseHandler = Hive.class;
        }

        delegationTokenManager.startDelegationTokenSecretManager(conf, baseHandler,
            HadoopThriftAuthBridge.Server.ServerMode.METASTORE);
        saslServer.setSecretManager(delegationTokenManager.getSecretManager());
      } catch (IOException e) {
        throw new TTransportException("Failed to start token manager", e);
      }

    }
  }

  public MetastoreDelegationTokenManager getDelegationTokenManager() {
    return delegationTokenManager;
  }

  public Server getSaslServer() {
    return saslServer;
  }

}
