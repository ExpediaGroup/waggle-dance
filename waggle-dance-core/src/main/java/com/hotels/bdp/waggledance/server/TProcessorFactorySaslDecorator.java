/**
 * Copyright (C) 2016-2019 Expedia, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.bdp.waggledance.server;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


public class TProcessorFactorySaslDecorator extends TProcessorFactory {

    private  HadoopThriftAuthBridge.Server saslServer;
    private  TProcessorFactory tProcessorFactory;

    public TProcessorFactorySaslDecorator(
            TProcessorFactory tProcessorFactory,
            HiveConf hiveConf) throws TTransportException {
        super(null);
        this.tProcessorFactory = tProcessorFactory;
        HadoopThriftAuthBridge hadoopThriftAuthBridge = ShimLoader.getHadoopThriftAuthBridge();
        this.saslServer = hadoopThriftAuthBridge.createServer(hiveConf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_KEYTAB_FILE),
                hiveConf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL));
    }

    public  HadoopThriftAuthBridge.Server getSaslServer() {
        return saslServer;
    }

    @Override
    public TProcessor getProcessor(TTransport transport) {
        try {
            TProcessor tProcessor = tProcessorFactory.getProcessor(transport);
            return saslServer.wrapProcessor(tProcessor);

        } catch (RuntimeException e) {
            throw new RuntimeException("Error creating SASL wrapped TProcessor", e);
        }
    }

}
