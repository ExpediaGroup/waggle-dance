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
package com.hotels.bdp.waggledance.manifest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.ProtectionDomain;
import java.util.Enumeration;
import java.util.Map.Entry;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read and make available all the attributes held in the specified class' META_INF/MANIFEST.MF file. The attributes are
 * made available via a getter and the toString method can for instance be used on application start to report build
 * date, build user and version number of Maven builds.
 * <p>
 * This class does the best effort to find the correct manifest in the classpath.
 * </p>
 */
public class ManifestAttributes {
  private static Logger LOG = LoggerFactory.getLogger(ManifestAttributes.class);

  static final String META_INF_MANIFEST_MF = "META-INF/MANIFEST.MF";
  private static final String JAR_PROTOCOL = "jar:";
  private static final String FILE_PROTOCOL = "file:";
  private final String attributesString;

  private Attributes attributes = new Attributes();

  public ManifestAttributes(Class<?> mainClass) {
    this(mainClass.getProtectionDomain());
  }

  ManifestAttributes(ProtectionDomain protectionDomain) {
    StringBuilder attributesStringBuilder = new StringBuilder();
    try (InputStream manifestStream = openManifestStream(protectionDomain)) {
      if (manifestStream != null) {
        attributes = new Manifest(manifestStream).getMainAttributes();
        for (Entry<Object, Object> entry : attributes.entrySet()) {
          attributesStringBuilder.append(entry.getKey() + "=" + entry.getValue() + "; ");
        }
      } else {
        attributesStringBuilder.append("Could not find Manifest via " + protectionDomain);
      }
    } catch (NullPointerException e) {
      LOG.warn("No Manifest found", e);
      attributesStringBuilder.append("No Manifest found");
    } catch (Exception e) {
      LOG.warn("Error getting manifest", e);
      attributesStringBuilder.append("Error getting manifest " + e.getMessage());
    }

    attributesString = attributesStringBuilder.toString();
  }

  public String getAttribute(String name) {
    return attributes.getValue(new Attributes.Name(name));
  }

  protected InputStream openManifestStream(ProtectionDomain protectionDomain)
    throws MalformedURLException, IOException {
    URL manifestUrl = null;

    // try to pick the Manifest in the source JAR
    manifestUrl = selectManifestFromJars(protectionDomain);
    LOG.debug("Manifest location in JARs is {}", manifestUrl);

    if (manifestUrl == null) {
      // if we can't locate the correct JAR then try get to manifest file via a file path (e.g. in Hadoop case where
      // jar is unpacked to disk)
      manifestUrl = selectFromFileLocation(protectionDomain);
      LOG.debug("Manifest location on disk is {}", manifestUrl);
    }

    if (manifestUrl == null) {
      // file not found, get via class loader resource (e.g. from inside jar)
      manifestUrl = protectionDomain.getClassLoader().getResource(META_INF_MANIFEST_MF);
      LOG.debug("Manifest location via getResource() is {}", manifestUrl);
    }

    if (manifestUrl == null) {
      LOG.warn("Manifest not found!");
      return null;
    }

    return manifestUrl.openStream();
  }

  private URL selectManifestFromJars(ProtectionDomain protectionDomain) throws IOException {
    URL jarLocation = protectionDomain.getCodeSource().getLocation();
    if (jarLocation != null) {
      String containingJar = JAR_PROTOCOL + jarLocation.toString();
      Enumeration<URL> resources = protectionDomain.getClassLoader().getResources(META_INF_MANIFEST_MF);
      if (resources != null) {
        while (resources.hasMoreElements()) {
          URL url = resources.nextElement();
          if (url.toString().startsWith(containingJar)) {
            LOG.debug("Found a manifest in location {}", url);
            return url;
          }
        }
      }
    }
    return null;
  }

  private URL selectFromFileLocation(ProtectionDomain protectionDomain) throws IOException {
    String location = protectionDomain.getCodeSource().getLocation().getFile();
    location = FILE_PROTOCOL + location + "/" + META_INF_MANIFEST_MF;
    URL url = new URL(location);
    File manifestFile = new File(url.getFile());
    if (manifestFile.exists()) {
      return url;
    }
    LOG.debug("Could not find manifest in location {}", location);
    return null;
  }

  @Override
  public String toString() {
    return attributesString;
  }

}
