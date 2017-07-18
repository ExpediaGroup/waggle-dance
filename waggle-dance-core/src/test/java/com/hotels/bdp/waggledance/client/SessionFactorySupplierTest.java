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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.File;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import com.google.common.collect.ImmutableList;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.KeyPair;
import com.pastdev.jsch.DefaultSessionFactory;
import com.pastdev.jsch.SessionFactory;

import com.hotels.bdp.waggledance.api.WaggleDanceException;

public class SessionFactorySupplierTest {

  private static final int SSH_PORT = 22;
  private static final String KNOWN_HOSTS = "knownHosts";
  private static final String IDENTITY_KEY_1 = "K1";
  private static final String IDENTITY_KEY_2 = "K2";

  public @Rule TemporaryFolder tmpFolder = new TemporaryFolder();

  private File knownHosts;
  private File identityKey1;
  private File identityKey2;

  @Before
  public void init() throws Exception {
    knownHosts = tmpFolder.newFile(KNOWN_HOSTS);

    JSch jSch = new JSch();
    KeyPair keyPair = null;

    identityKey1 = tmpFolder.newFile(IDENTITY_KEY_1);
    keyPair = KeyPair.genKeyPair(jSch, KeyPair.RSA);
    keyPair.writePrivateKey(identityKey1.getAbsolutePath());

    identityKey2 = tmpFolder.newFile(IDENTITY_KEY_2);
    keyPair = KeyPair.genKeyPair(jSch, KeyPair.RSA);
    keyPair.writePrivateKey(identityKey2.getAbsolutePath());
  }

  @Test
  public void typical() {
    SessionFactory sessionFactory = new SessionFactorySupplier(SSH_PORT, knownHosts.getAbsolutePath(),
        ImmutableList.of(identityKey1.getAbsolutePath(), identityKey2.getAbsolutePath())).get();
    assertThat(sessionFactory.getClass().isAssignableFrom(DefaultSessionFactory.class), is(true));
  }

  @Test
  public void invalidPort() {
    new SessionFactorySupplier(-1, "hosts",
        ImmutableList.of(identityKey1.getAbsolutePath(), identityKey2.getAbsolutePath())).get();
  }

  @Test
  public void invalidKnownHosts() {
    new SessionFactorySupplier(SSH_PORT, "hosts",
        ImmutableList.of(identityKey1.getAbsolutePath(), identityKey2.getAbsolutePath())).get();
  }

  @Test(expected = WaggleDanceException.class)
  public void invalidIdentityKey() {
    new SessionFactorySupplier(SSH_PORT, knownHosts.getAbsolutePath(), ImmutableList.of("K1")).get();
  }

  @Test
  public void singleInstance() {
    SessionFactorySupplier sessionFactorySupplier = new SessionFactorySupplier(SSH_PORT, knownHosts.getAbsolutePath(),
        ImmutableList.of(identityKey1.getAbsolutePath(), identityKey2.getAbsolutePath()));
    SessionFactory sessionFactoryA = sessionFactorySupplier.get();
    SessionFactory sessionFactoryB = sessionFactorySupplier.get();
    assertThat(sessionFactoryA, is(sessionFactoryB));
  }

}
