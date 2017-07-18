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

import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveConfFactory {

  private static final Logger LOG = LoggerFactory.getLogger(HiveConfFactory.class);

  private final List<String> resources;
  private final Map<String, String> properties;

  public HiveConfFactory(List<String> resources, Map<String, String> properties) {
    this.resources = resources;
    this.properties = properties;
  }

  public HiveConf newInstance() {
    HiveConf conf;

    synchronized (HiveConf.class) {
      if (resources != null) {
        // The following prevents HiveConf from loading the default hadoop
        // *-site.xml and hive-site.xml if they're on the classpath.
        URL hiveSiteLocation = HiveConf.getHiveSiteLocation();
        HiveConf.setHiveSiteLocation(null);
        conf = new HiveConf(new Configuration(false), getClass());
        HiveConf.setHiveSiteLocation(hiveSiteLocation);
        for (String resource : resources) {
          LOG.debug("Adding custom resource: {}", resource);
          conf.addResource(resource);
        }
      } else {
        conf = new HiveConf(new Configuration(true), getClass());
      }
    }

    if (properties != null) {
      for (Entry<String, String> entry : properties.entrySet()) {
        LOG.debug("Adding custom property: {}={}", entry.getKey(), entry.getValue());
        conf.set(entry.getKey(), entry.getValue());
      }
    }

    return conf;
  }

}
