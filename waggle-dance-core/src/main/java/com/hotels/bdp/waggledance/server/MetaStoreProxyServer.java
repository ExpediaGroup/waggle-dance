/**
 * Copyright (C) 2015-2017 The Apache Software Foundation and Expedia Inc.
 *
 * This code is based on Hive's HiveMetaStore:
 *
 * https://github.com/apache/hive/blob/rel/release-2.3.0/metastore/src/java/org/apache/hadoop/hive/metastore/
 * HiveMetaStore.java
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hotels.bdp.waggledance.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.PreDestroy;
import javax.security.auth.login.LoginException;

import org.apache.hadoop.hive.common.auth.HiveAuthUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TServerSocketKeepAlive;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hive.service.auth.HiveAuthFactory;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.TTransportFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import com.hotels.bdp.waggledance.conf.WaggleDanceConfiguration;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class MetaStoreProxyServer implements ApplicationRunner {

  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreProxyServer.class);

  /**
   * default port on which to start the server (48869)
   */
  public static final int DEFAULT_WAGGLEDANCE_PORT = 0xBEE5;
  public static final String ADMIN = "admin";
  public static final String PUBLIC = "public";

  private final HiveConf hiveConf;
  private final WaggleDanceConfiguration waggleDanceConfiguration;
  private final TProcessorFactory tProcessorFactory;
  private final Lock startLock;
  private final Condition startCondition;
  private TServer tServer;

  @Autowired
  public MetaStoreProxyServer(
      HiveConf hiveConf,
      WaggleDanceConfiguration waggleDanceConfiguration,
      TProcessorFactory tProcessorFactory) {
    this.hiveConf = hiveConf;
    this.waggleDanceConfiguration = waggleDanceConfiguration;
    this.tProcessorFactory = tProcessorFactory;
    startLock = new ReentrantLock();
    startCondition = startLock.newCondition();
  }

  private boolean isRunning() {
    if (tServer == null) {
      return false;
    }
    return tServer.isServing();
  }

  @Override
  public void run(ApplicationArguments args) throws Exception {
    if (isRunning()) {
      throw new RuntimeException("Can't run more than one instance");
    }

    final boolean isCliVerbose = waggleDanceConfiguration.isVerbose();

    try {
      String msg = "Starting WaggleDance on port " + waggleDanceConfiguration.getPort();
      LOG.info(msg);
      if (waggleDanceConfiguration.isVerbose()) {
        System.err.println(msg);
      }

      // Add shutdown hook.
      Runtime.getRuntime().addShutdownHook(new Thread(() -> {
        String shutdownMsg = "Shutting down WaggleDance.";
        LOG.info(shutdownMsg);
        if (isCliVerbose) {
          System.err.println(shutdownMsg);
        }
      }));

      AtomicBoolean startedServing = new AtomicBoolean();
      startWaggleDance(startLock, startCondition, startedServing);
    } catch (Throwable t) {
      // Catch the exception, log it and rethrow it.
      LOG.error("WaggleDance Thrift Server threw an exception...", t);
      throw new Exception(t);
    }
  }

  /**
   * Start Metastore based on a passed {@link HadoopThriftAuthBridge}
   *
   * @param startLock
   * @param startCondition
   * @param startedServing
   * @throws Throwable
   */
  private void startWaggleDance(
      Lock startLock,
      Condition startCondition,
      AtomicBoolean startedServing)
    throws Throwable {
    try {
      // Server will create new threads up to max as necessary. After an idle
      // period, it will destory threads to keep the number of threads in the
      // pool to min.
      int minWorkerThreads = hiveConf.getIntVar(ConfVars.METASTORESERVERMINTHREADS);
      int maxWorkerThreads = hiveConf.getIntVar(ConfVars.METASTORESERVERMAXTHREADS);
      boolean tcpKeepAlive = hiveConf.getBoolVar(ConfVars.METASTORE_TCP_KEEP_ALIVE);
      boolean useFramedTransport = hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT);
      boolean useSSL = hiveConf.getBoolVar(ConfVars.HIVE_METASTORE_USE_SSL);
      boolean useSASL = hiveConf.getBoolVar(ConfVars.METASTORE_USE_THRIFT_SASL);

      TServerSocket serverSocket = createServerSocket(useSSL, waggleDanceConfiguration.getPort());

      if (tcpKeepAlive) {
        serverSocket = new TServerSocketKeepAlive(serverSocket);
      }

      TTransportFactory transFactory = createTTransportFactory(useFramedTransport, useSASL);
      TProcessorFactory tProcessorFactory = getTProcessorFactory(useSASL);
      LOG.info("Starting WaggleDance Server");

      TThreadPoolServer.Args args = new TThreadPoolServer.Args(serverSocket)
          .processorFactory(tProcessorFactory)
          .transportFactory(transFactory)
          .protocolFactory(new TBinaryProtocol.Factory())
          .minWorkerThreads(minWorkerThreads)
          .maxWorkerThreads(maxWorkerThreads)
          .stopTimeoutVal(waggleDanceConfiguration.getThriftServerStopTimeoutValInSeconds())
          .requestTimeout(waggleDanceConfiguration.getThriftServerRequestTimeout())
          .requestTimeoutUnit(waggleDanceConfiguration.getThriftServerRequestTimeoutUnit());

      tServer = new TThreadPoolServer(args);
      LOG.info("Started the new WaggleDance on port [" + waggleDanceConfiguration.getPort() + "]...");
      LOG.info("Options.minWorkerThreads = " + minWorkerThreads);
      LOG.info("Options.maxWorkerThreads = " + maxWorkerThreads);
      LOG.info("TCP keepalive = " + tcpKeepAlive);

      if (startLock != null) {
        signalOtherThreadsToStart(tServer, startLock, startCondition, startedServing);
      }
      tServer.serve();
    } catch (Throwable x) {
      LOG.error(StringUtils.stringifyException(x));
      throw x;
    }
    LOG.info("Waggle Dance has stopped");
  }

  private TProcessorFactory getTProcessorFactory(boolean useSASL) throws TTransportException {
    if (useSASL) {
      return new TProcessorFactorySaslDecorator(tProcessorFactory, hiveConf);
    } else {
      return tProcessorFactory;
    }
  }

  private TTransportFactory createTTransportFactory(boolean useFramedTransport, boolean useSASL)
          throws TTransportException, LoginException {
    TTransportFactory transFactory;
    if (useFramedTransport) {
      transFactory = new TFramedTransport.Factory();
    } else {
      transFactory = new TTransportFactory();
    }
    if (useSASL) {
      UserGroupInformation.setConfiguration(hiveConf);
      transFactory = new HiveAuthFactory(hiveConf).getAuthTransFactory();
    }
    return transFactory;
  }

  private TServerSocket createServerSocket(boolean useSSL, int port) throws IOException, TTransportException {
    TServerSocket serverSocket = null;
    // enable SSL support for HMS
    List<String> sslVersionBlacklist = new ArrayList<>(Arrays.asList(hiveConf.getVar(ConfVars.HIVE_SSL_PROTOCOL_BLACKLIST).split(",")));
    if (!useSSL) {
      serverSocket = HiveAuthUtils.getServerSocket(null, port);
    } else {
      String keyStorePath = hiveConf.getVar(ConfVars.HIVE_METASTORE_SSL_KEYSTORE_PATH).trim();
      if (keyStorePath.isEmpty()) {
        throw new IllegalArgumentException(
            ConfVars.HIVE_METASTORE_SSL_KEYSTORE_PASSWORD.varname + " Not configured for SSL connection");
      }
      String keyStorePassword = ShimLoader
          .getHadoopShims()
          .getPassword(hiveConf, HiveConf.ConfVars.HIVE_METASTORE_SSL_KEYSTORE_PASSWORD.varname);
      serverSocket = HiveAuthUtils.getServerSSLSocket(null, port, keyStorePath, keyStorePassword, sslVersionBlacklist);
    }
    return serverSocket;
  }

  private void signalOtherThreadsToStart(
      final TServer server,
      final Lock startLock,
      final Condition startCondition,
      final AtomicBoolean startedServing) {
    // A simple thread to wait until the server has started and then signal the other threads to
    // begin
    Thread t = new Thread(() -> {
      do {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          LOG.warn("Signalling thread was interuppted: " + e.getMessage());
        }
      } while (!server.isServing());
      startLock.lock();
      try {
        startedServing.set(true);
        startCondition.signalAll();
      } finally {
        startLock.unlock();
      }
    });
    t.start();
  }

  @PreDestroy
  public void stop() {
    if (tServer == null) {
      return;
    }
    tServer.stop();
    tServer = null;
  }

  public void waitUntilStarted() throws InterruptedException {
    waitUntilStarted(3, 1, TimeUnit.MINUTES);
  }

  public void waitUntilStarted(int retries, long waitDelay, TimeUnit waitDelayTimeUnit) throws InterruptedException {
    if (isRunning()) {
      return;
    }
    int i = 0;
    while (i < retries) {
      i++;
      startLock.lock();
      try {
        if (startCondition.await(waitDelay, waitDelayTimeUnit)) {
          break;
        }
      } finally {
        startLock.unlock();
      }
      if (i == retries) {
        throw new RuntimeException("Maximum number of tries reached whilst waiting for Thrift server to be ready");
      }
    }
  }

}
