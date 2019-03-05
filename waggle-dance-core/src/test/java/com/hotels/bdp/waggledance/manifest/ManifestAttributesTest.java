/**
 * Copyright (C) 2016-2017 Expedia, Inc.
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
package com.hotels.bdp.waggledance.manifest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.powermock.api.mockito.PowerMockito.when;

import java.net.URL;
import java.security.CodeSource;
import java.security.ProtectionDomain;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import fm.last.commons.test.file.ClassDataFolder;

import com.google.common.base.Preconditions;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ManifestAttributes.class })
@PowerMockIgnore("javax.management.*")
public class ManifestAttributesTest {

  private @Rule ClassDataFolder dataFolder = new ClassDataFolder();

  public @Mock ClassLoader clazzLoader;
  public @Mock CodeSource clazzCodeSource;
  public @Mock ProtectionDomain clazzProtectionDomain;

  @Before
  public void init() throws Exception {
    when(clazzCodeSource.getLocation()).thenReturn(new URL("file://./"));
    when(clazzProtectionDomain.getCodeSource()).thenReturn(clazzCodeSource);
    when(clazzProtectionDomain.getClassLoader()).thenReturn(clazzLoader);
  }

  @Test
  public void test() {
    // we can't find a manifest as there is no jar here but below at least
    // shouldn't throw an exception
    new ManifestAttributes(clazzProtectionDomain).toString();
  }

  @Test
  public void ensureAttributesAreAvailable() throws Exception {
    when(clazzLoader.getResource(ManifestAttributes.META_INF_MANIFEST_MF))
        .thenReturn(dataFolder.getFile("TEST_MANIFEST.MF").toURI().toURL());
    ManifestAttributes manifestAttributes = new ManifestAttributes(clazzProtectionDomain);

    assertThat(manifestAttributes.getAttribute("Maven-GroupId"), is("com.hotels.bdp.waggledance"));
    assertThat(manifestAttributes.getAttribute("Maven-ArtifactId"), is("waggle-dance"));
    assertThat(manifestAttributes.getAttribute("Build-Version"), is("0.0.1-SNAPSHOT"));
  }

  @Test
  public void noManifestFound() throws Exception {
    ManifestAttributes manifestAttributes = new ManifestAttributes(clazzProtectionDomain);
    assertThat(manifestAttributes.toString(), startsWith("Could not find Manifest"));
  }

  @Test
  public void exceptionWhileReadingManifest() throws Exception {
    when(clazzLoader.getResource(ManifestAttributes.META_INF_MANIFEST_MF)).thenThrow(new IllegalStateException());
    ManifestAttributes manifestAttributes = new ManifestAttributes(clazzProtectionDomain);
    assertThat(manifestAttributes.toString(), startsWith("Error getting manifest"));
  }

  @Test
  public void loadFromSourceJar() {
    ManifestAttributes manifestAttributes = new ManifestAttributes(Preconditions.class);

    assertThat(manifestAttributes.getAttribute("Bundle-Name"), is("Guava: Google Core Libraries for Java"));
    assertThat(manifestAttributes.getAttribute("Bundle-SymbolicName"), is("com.google.guava"));
  }

  @Test
  public void loadFromSourceDirectory() throws Exception {
    when(clazzCodeSource.getLocation()).thenReturn(dataFolder.getFolder().toURI().toURL());
    ManifestAttributes manifestAttributes = new ManifestAttributes(clazzProtectionDomain);

    assertThat(manifestAttributes.getAttribute("Maven-GroupId"), is("com.hotels.bdp.waggledance"));
    assertThat(manifestAttributes.getAttribute("Maven-ArtifactId"), is("good-job"));
  }

}
