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
package com.hotels.bdp.waggledance.server.glue.aws;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.AWSGlueClientBuilder;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public final class AWSGlueClientFactory implements GlueClientFactory {
  private static final Logger logger = Logger.getLogger(AWSGlueClientFactory.class);

  public static final String AWS_GLUE_ENDPOINT = "aws.com.hotels.bdp.waggledance.glue.endpoint";

  public static final String AWS_REGION = "aws.region";
  public static final String AWS_CATALOG_CREDENTIALS_PROVIDER_FACTORY_CLASS = "aws.catalog.aws.provider.factory.class";
  private final HiveConf conf;

  public AWSGlueClientFactory(HiveConf conf) {
    Preconditions.checkNotNull(conf, "HiveConf cannot be null");
    this.conf = conf;
  }

  private AWSCredentialsProvider getAWSCredentialsProvider(HiveConf conf) {
    Class<? extends AWSCredentialsProviderFactory> providerFactoryClass = conf
        .getClass("aws.catalog.aws.provider.factory.class", DefaultAWSCredentialsProviderFactory.class)
        .asSubclass(AWSCredentialsProviderFactory.class);

    AWSCredentialsProviderFactory provider = (AWSCredentialsProviderFactory) ReflectionUtils
        .newInstance(providerFactoryClass, conf);

    return provider.buildAWSCredentialsProvider(conf);
  }

  public AWSGlue newClient() throws MetaException {
    try {
      AWSGlueClientBuilder glueClientBuilder = (AWSGlueClientBuilder) AWSGlueClientBuilder.standard().withCredentials(
          getAWSCredentialsProvider(conf));

      String regionStr = getProperty("aws.region", conf);
      String glueEndpoint = getProperty("aws.com.hotels.bdp.waggledance.glue.endpoint", conf);

      if (StringUtils.isNotBlank(glueEndpoint)) {
        logger.info("Setting com.hotels.bdp.waggledance.glue service endpoint to " + glueEndpoint);
        glueClientBuilder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(glueEndpoint, null));
      } else if (StringUtils.isNotBlank(regionStr)) {
        logger.info("Setting region to : " + regionStr);
        glueClientBuilder.setRegion(regionStr);
      } else {
        Region currentRegion = Regions.getCurrentRegion();
        if (currentRegion != null) {
          logger.info("Using region from ec2 metadata : " + currentRegion.getName());
          glueClientBuilder.setRegion(currentRegion.getName());
        } else {
          logger.info("No region info found, using SDK default region: us-east-1");
        }
      }

      return (AWSGlue) glueClientBuilder.build();
    } catch (Exception e) {
      String message = "Unable to build AWSGlueClient: " + e;
      logger.error(message);
      throw new MetaException(message);
    }
  }

  private static String getProperty(String propertyName, HiveConf conf) {
    return Strings.isNullOrEmpty(System.getProperty(propertyName)) ? conf.get(propertyName)
        : System.getProperty(propertyName);
  }
}
