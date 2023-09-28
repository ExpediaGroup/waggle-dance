/**
 * Copyright (C) 2016-2023 Expedia, Inc.
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
package com.hotels.bdp.waggledance.server.security.ranger;

import static com.hotels.bdp.waggledance.server.security.ranger.WaggleDanceRangerUtil.*;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.ranger.authorization.hadoop.config.RangerConfiguration;
import org.apache.ranger.plugin.policyengine.RangerAccessResult;
import org.apache.ranger.plugin.service.RangerBasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.hotels.bdp.waggledance.server.security.NotAllowedException;

public class RangerAccessControlHandlerImpl implements RangerAccessControlHandler {
  private static final Logger LOG = LoggerFactory.getLogger(RangerAccessControlHandlerImpl.class);

  private static Cache<String, Boolean> accessCache;
  private final RangerSQLPlugin plugin;

  public RangerAccessControlHandlerImpl(HiveConf conf) {
    String adminUri = conf.get(RANGER_REST_URL);
    URI adminURI = URI.create(adminUri);

    String cacheDir = conf.get(CACHE_DIR);
    String rangerService = conf.get(RANGER_SERVICE_NAME);
    LOG.info(String.format("Init ranger plugun , ranger admin uri : %s ,cache dir %s ,ranger service %s"
            , adminUri, cacheDir, rangerService));

    RangerConfiguration rangerConfiguration = RangerConfiguration.getInstance();
    rangerConfiguration.addResource(conf);

    rangerConfiguration.set(RANGER_REST_URL, adminURI.toString());
    if (cacheDir != null) {
      LOG.info("Using Ranger policy cache dir `{}`", cacheDir);
      Path path = Paths.get(cacheDir);
      if (Files.notExists(path)) {
        try {
          Files.createDirectory(path);
          rangerConfiguration.set(CACHE_DIR, cacheDir);
        } catch (IOException ex) {
          LOG.warn("Failed to ensure Ranger cache dir `{}`. Run without caching", path, ex);
        }
      } else {
        rangerConfiguration.set(CACHE_DIR, cacheDir);
      }
    } else {
      LOG.warn("Running GDC SQL Ranger Authorizer without policy caching");
    }

    if (rangerService == null)
      throw new IllegalArgumentException(String.format(
              "Ranger policy service configuration was not found"));
    LOG.info("Using Ranger policy service `{}`, Admin=`{}`",
            rangerService, "adminUri");
    rangerConfiguration.set(RANGER_SERVICE_NAME, rangerService);
    rangerConfiguration.set(
            RANGER_SOURCE_IMPL,
            "org.apache.ranger.admin.client.RangerAdminRESTClient");
    rangerConfiguration.set(RANGER_POLICY_POLLINTERVAL,
            conf.get(RANGER_POLICY_POLLINTERVAL, "180000"));
    UserGroupInformation.setConfiguration(rangerConfiguration);

    if (accessCache == null) {
      //TODO: make parameters configurable
      accessCache = CacheBuilder.newBuilder()
              .concurrencyLevel(5)
              .initialCapacity(100)
              .maximumSize(3000)
              .expireAfterWrite(600, TimeUnit.SECONDS)
              .removalListener(notification -> {
                LOG.info(String.format("key %s value %s be removed, because %s", notification.getKey(), notification.getValue(),
                        notification.getCause()));
              })
              .build();
    }

    this.plugin = new RangerSQLPlugin(SERVICE_TYPE);
    this.plugin.init();
  }

  private boolean getPermissionWithCache(String databaseName, String tableName, String user, AccessType type) {
    String typeString = String.format("%s_%s_%s_%s", databaseName, tableName, user, type.toString());
    return accessCache.getIfPresent(typeString) != null;
  }

  private void addPermission2Cache(String databaseName, String tableName, String user, AccessType type) {
    String typeString = String.format("%s_%s_%s_%s", databaseName, tableName, user, type.toString());
    accessCache.put(typeString, true);
  }

  @Override
  public boolean hasUpdatePermission(String databaseName, String tableName, String user, Set<String> groups) {
    return checkTablePermissionInternal(databaseName, tableName, user, groups, AccessType.UPDATE);
  }

  @Override
  public boolean hasCreatePermission(String databaseName, String tableName, String user, Set<String> groups) {
    return checkTablePermissionInternal(databaseName, tableName, user, groups, AccessType.CREATE);
  }

  @Override
  public boolean hasSelectPermission(String databaseName, String tableName, String user, Set<String> groups) {
    return checkTablePermissionInternal(databaseName, tableName, user, groups, AccessType.SELECT);
  }

  @Override
  public boolean hasDropPermission(String databaseName, String tableName, String user, Set<String> groups) {
    return checkTablePermissionInternal(databaseName, tableName, user, groups, AccessType.DROP);
  }

  @Override
  public boolean hasAlterPermission(String databaseName, String tableName, String user, Set<String> groups) {
    return checkTablePermissionInternal(databaseName, tableName, user, groups, AccessType.ALTER);
  }

  private boolean checkTablePermissionInternal(String databaseName, String tableName,
                                               String user, Set<String> groups, AccessType accessType) {
    if (getPermissionWithCache(databaseName, tableName, user, accessType))
      return true;
    Resource resource = new Resource(Resource.Type.TABLE, databaseName, tableName);
    AccessRequest accRequest = new AccessRequest(resource, accessType, user, groups);
    RangerAccessResult result = plugin.isAccessAllowed(accRequest);
    if (result != null || !result.getIsAllowed()) {
      throw new NotAllowedException(String.format(
              "ranger authorization failed, User %s, User group %s, db %s table %s, accessType %s",
              accRequest.getUser(), accRequest.getUserGroups(),
              accRequest.getResource().getValue("database"),
              accRequest.getResource().getValue("table"), accRequest.getAccessType()
      ));
    }
    addPermission2Cache(databaseName, tableName, user, AccessType.ALTER);
    return true;
  }

  static class RangerSQLPlugin extends RangerBasePlugin {
    public RangerSQLPlugin(String service) {
      super(service, service);
    }
  }

}
