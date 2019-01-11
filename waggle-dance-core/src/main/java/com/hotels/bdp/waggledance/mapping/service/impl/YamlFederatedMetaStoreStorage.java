/**
 * Copyright (C) 2016-2019 Expedia Inc.
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
package com.hotels.bdp.waggledance.mapping.service.impl;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.Validator;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.FederatedMetaStore;
import com.hotels.bdp.waggledance.api.model.FederationType;
import com.hotels.bdp.waggledance.api.model.Federations;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;
import com.hotels.bdp.waggledance.conf.YamlStorageConfiguration;
import com.hotels.bdp.waggledance.mapping.service.FederatedMetaStoreStorage;
import com.hotels.bdp.waggledance.yaml.YamlFactory;

@Repository
public class YamlFederatedMetaStoreStorage implements FederatedMetaStoreStorage {
  private static final Logger LOG = LoggerFactory.getLogger(YamlFederatedMetaStoreStorage.class);

  private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

  static class YamlMarshaller {
    private final FileSystemManager fsManager;
    private final Yaml yaml;

    YamlMarshaller() {
      try {
        fsManager = VFS.getManager();
      } catch (FileSystemException e) {
        throw new RuntimeException("Unable to initialize Virtual File System", e);
      }
      yaml = YamlFactory.newYaml();
    }

    public Federations unmarshall(String federationConfigLocation) {
      try (FileObject source = fsManager.resolveFile(federationConfigLocation)) {
        Federations federations = (Federations) yaml.load(source.getContent().getInputStream());
        if (federations == null) {
          return null;
        }
        validate(federations);
        return federations;
      } catch (IOException e) {
        throw new RuntimeException("Unable to initialize federations from '" + federationConfigLocation + "'", e);
      }
    }

    public void marshall(String federationConfigLocation, Federations federations) {
      try (FileObject target = fsManager.resolveFile(federationConfigLocation);
          Writer writer = new OutputStreamWriter(target.getContent().getOutputStream(), Charsets.UTF_8)) {
        yaml.dump(federations, writer);
      } catch (IOException e) {
        throw new RuntimeException("Unable to write federations to '" + federationConfigLocation + "'", e);
      }
    }

  }

  private static void validate(Federations federations) {
    Set<ConstraintViolation<Federations>> constraintViolations = VALIDATOR.validate(federations);
    if (!constraintViolations.isEmpty()) {
      throw new ConstraintViolationException("Invalid federated metastore", constraintViolations);
    }
  }

  private static void validate(AbstractMetaStore federatedMetaStore) {
    Set<ConstraintViolation<AbstractMetaStore>> constraintViolations = VALIDATOR.validate(federatedMetaStore);
    if (!constraintViolations.isEmpty()) {
      throw new ConstraintViolationException("Invalid federated metastore", constraintViolations);
    }
  }

  private static void insert(AbstractMetaStore federatedMetaStore, Map<String, AbstractMetaStore> federationsMap) {
    validate(federatedMetaStore);
    if (federationsMap.containsKey(federatedMetaStore.getName())) {
      throw new IllegalArgumentException("Name '" + federatedMetaStore.getName() + "' is already registered");
    }
    if (!uniqueMetaStorePrefix(federatedMetaStore.getDatabasePrefix(), federationsMap)) {
      throw new IllegalArgumentException(
          "Prefix '" + federatedMetaStore.getDatabasePrefix() + "' is already registered");
    }
    LOG.info("Adding federation {}", federatedMetaStore);
    federationsMap.put(federatedMetaStore.getName(), federatedMetaStore);
  }

  private static boolean uniqueMetaStorePrefix(String prefix, Map<String, AbstractMetaStore> federationsMap) {
    for (AbstractMetaStore metaStore : federationsMap.values()) {
      if (metaStore.getDatabasePrefix().equalsIgnoreCase(prefix)) {
        return false;
      }
    }
    return true;
  }

  private final Object federationsMapLock = new Object();
  private final String federationConfigLocation;
  private final YamlMarshaller yamlMarshaller;
  private Map<String, AbstractMetaStore> federationsMap;
  private PrimaryMetaStore primaryMetaStore;
  private final boolean writeConfigOnShutdown;

  @Autowired
  public YamlFederatedMetaStoreStorage(
      @Value("${federation-config}") String federationConfigLocation,
      YamlStorageConfiguration configuration) {
    this(federationConfigLocation, new YamlMarshaller(), configuration.isOverwriteConfigOnShutdown());
  }

  YamlFederatedMetaStoreStorage(
      String federationConfigLocation,
      YamlMarshaller yamlSerializer,
      boolean writeConfigOnShutdown) {
    this.federationConfigLocation = federationConfigLocation;
    this.writeConfigOnShutdown = writeConfigOnShutdown;
    yamlMarshaller = yamlSerializer;
    federationsMap = new LinkedHashMap<>();
  }

  @PostConstruct
  public void loadFederation() {
    LOG.info("Loading federations from {}", federationConfigLocation);
    Map<String, AbstractMetaStore> newFederationsMap = new LinkedHashMap<>();
    Federations federations = yamlMarshaller.unmarshall(federationConfigLocation);
    if (federations != null && federations.getPrimaryMetaStore() != null) {
      primaryMetaStore = federations.getPrimaryMetaStore();
      insert(primaryMetaStore, newFederationsMap);
    }
    if (federations != null && federations.getFederatedMetaStores() != null) {
      for (AbstractMetaStore federatedMetaStore : federations.getFederatedMetaStores()) {
        if (federatedMetaStore.getFederationType() == FederationType.PRIMARY) {
          // This can only happen through wrong manual configuration
          throw new RuntimeException("Found 'PRIMARY' metastore that should be configured as 'FEDERATED'");
        }
        insert(federatedMetaStore, newFederationsMap);
      }
    }
    synchronized (federationsMapLock) {
      federationsMap = newFederationsMap;
    }
    LOG.info("Loaded {} federations", federationsMap.size());
  }

  @PreDestroy
  public void saveFederation() {
    if (writeConfigOnShutdown) {
      yamlMarshaller
          .marshall(federationConfigLocation, new Federations(getPrimaryMetaStore(), getAllFederatedMetaStores()));
    }
  }

  @Override
  public void insert(AbstractMetaStore federatedMetaStore) {
    validate(federatedMetaStore);
    synchronized (federationsMapLock) {
      if (federatedMetaStore.getFederationType() == FederationType.PRIMARY) {
        primaryMetaStore = (PrimaryMetaStore) federatedMetaStore;
      }
      insert(federatedMetaStore, federationsMap);
    }
  }

  @Override
  public void update(AbstractMetaStore oldMetaStore, AbstractMetaStore newMetaStore) {
    validate(newMetaStore);
    synchronized (federationsMapLock) {
      LOG.debug("Updating federation {} to {}", oldMetaStore, newMetaStore);
      if (newMetaStore.getFederationType() == FederationType.PRIMARY) {
        primaryMetaStore = (PrimaryMetaStore) newMetaStore;
      }
      federationsMap.remove(oldMetaStore.getName());
      federationsMap.put(newMetaStore.getName(), newMetaStore);
    }
  }

  @Override
  public AbstractMetaStore delete(String name) {
    AbstractMetaStore federatedMetaStore;
    synchronized (federationsMapLock) {
      federatedMetaStore = federationsMap.remove(name);
      if (federatedMetaStore != null && federatedMetaStore.getFederationType() == FederationType.PRIMARY) {
        primaryMetaStore = null;
      }
    }
    return federatedMetaStore;
  }

  @Override
  public List<AbstractMetaStore> getAll() {
    return ImmutableList.copyOf(federationsMap.values());
  }

  @Override
  public AbstractMetaStore get(String name) {
    return federationsMap.get(name);
  }

  private List<FederatedMetaStore> getAllFederatedMetaStores() {
    Builder<FederatedMetaStore> builder = ImmutableList.builder();
    for (AbstractMetaStore federatedMetaStore : federationsMap.values()) {
      if (federatedMetaStore.getFederationType() == FederationType.FEDERATED) {
        builder.add((FederatedMetaStore) federatedMetaStore);
      }
    }
    return builder.build();
  }

  private PrimaryMetaStore getPrimaryMetaStore() {
    return primaryMetaStore;
  }

}
