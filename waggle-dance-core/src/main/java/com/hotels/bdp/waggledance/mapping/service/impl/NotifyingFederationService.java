/**
 * Copyright (C) 2016-2019 Expedia, Inc.
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

import static com.hotels.hcommon.ssh.validation.Preconditions.checkIsTrue;
import static com.hotels.hcommon.ssh.validation.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.mapping.service.FederatedMetaStoreStorage;

@Service
public class NotifyingFederationService implements FederationService {

  private static final Logger LOG = LoggerFactory.getLogger(NotifyingFederationService.class);
  private final FederatedMetaStoreStorage federatedMetaStoreStorage;
  private final List<FederationEventListener> listeners;

  @Autowired
  public NotifyingFederationService(FederatedMetaStoreStorage federatedMetaStoreStorage) {
    this.federatedMetaStoreStorage = federatedMetaStoreStorage;
    listeners = Collections.synchronizedList(new ArrayList<>());
    LOG.info("Constructed NotifyingFederationService");
  }

  @PostConstruct
  public void postConstruct() {
    LOG.info("calling postConstruct");
    List<? extends AbstractMetaStore> federatedMetaStores = getAll();
    for (AbstractMetaStore federatedMetaStore : federatedMetaStores) {
      LOG.info("calling onRegister for {}", federatedMetaStore.getName());
      onRegister(federatedMetaStore);
    }
  }

  @PreDestroy
  public void preDestroy() {
    LOG.info("calling preDestroy");
    List<? extends AbstractMetaStore> federatedMetaStores = getAll();
    for (AbstractMetaStore federatedMetaStore : federatedMetaStores) {
      onUnregister(federatedMetaStore);
    }
  }

  public void subscribe(FederationEventListener listener) {
    listeners.add(listener);
  }

  public void unsubscribe(FederationEventListener listener) {
    listeners.remove(listener);
  }

  private void onRegister(AbstractMetaStore federatedMetaStore) {
    synchronized (listeners) {
      for (FederationEventListener listener : listeners) {
        listener.onRegister(federatedMetaStore);
      }
    }
  }

  private void onUpdate(AbstractMetaStore oldMetaStore, AbstractMetaStore newMetaStore) {
    synchronized (listeners) {
      for (FederationEventListener listener : listeners) {
        listener.onUpdate(oldMetaStore, newMetaStore);
      }
    }
  }

  private void onUnregister(AbstractMetaStore federatedMetaStore) {
    synchronized (listeners) {
      for (FederationEventListener listener : listeners) {
        listener.onUnregister(federatedMetaStore);
      }
    }
  }

  @Override
  public void register(@NotNull @Valid AbstractMetaStore metaStore) {
    checkNotNull(metaStore, "federatedMetaStore cannot be null");
    boolean metastoreDoesNotExist = federatedMetaStoreStorage.get(metaStore.getName()) == null;
    checkIsTrue(metastoreDoesNotExist, "MetaStore '" + metaStore + "' is already registered");
    LOG.debug("Registering new federation {}", metaStore);
    synchronized (federatedMetaStoreStorage) {
      federatedMetaStoreStorage.insert(metaStore);
      onRegister(metaStore);
    }
    LOG.info("New federation {} has been registered successfully", metaStore);
  }

  @Override
  public void update(AbstractMetaStore oldMetaStore, AbstractMetaStore newMetaStore) {
    checkNotNull(oldMetaStore, "old federatedMetaStore cannot be null");
    checkNotNull(newMetaStore, "new federatedMetaStore cannot be null");
    boolean metastoreExists = federatedMetaStoreStorage.get(oldMetaStore.getName()) != null;
    checkIsTrue(metastoreExists, "MetaStore '" + oldMetaStore + "' is not registered");
    if (!oldMetaStore.getName().equals(newMetaStore.getName())) {
      boolean newNameDoesNotExist = federatedMetaStoreStorage.get(newMetaStore.getName()) == null;
      checkIsTrue(newNameDoesNotExist, "MetaStore '" + newMetaStore + "' is already registered");
    }
    LOG.debug("Registering update of existing federation {} to {}", oldMetaStore, newMetaStore);
    synchronized (federatedMetaStoreStorage) {
      federatedMetaStoreStorage.update(oldMetaStore, newMetaStore);
      onUpdate(oldMetaStore, newMetaStore);
    }
    LOG.debug("Update of federation {} to {} has been registered successfully", oldMetaStore, newMetaStore);
  }

  @Override
  public void unregister(@NotNull String name) {
    checkNotNull(name, "name cannot be null");
    checkNotNull(federatedMetaStoreStorage.get(name), "MeataStore with name '" + name + "' is not registered");
    LOG.debug("Unregistering federation with name {}", name);
    synchronized (federatedMetaStoreStorage) {
      AbstractMetaStore federatedMetaStore = federatedMetaStoreStorage.delete(name);
      onUnregister(federatedMetaStore);
    }
    LOG.debug("Federation with name {} is no longer available", name);
  }

  @Override
  public AbstractMetaStore get(@NotNull String name) {
    AbstractMetaStore federatedMetaStore = federatedMetaStoreStorage.get(name);
    return checkNotNull(federatedMetaStore, "No federation with name '" + name + "' found");
  }

  @Override
  public List<AbstractMetaStore> getAll() {
    LOG.info("NotifyingFederationService.getAll() was called");
    List<AbstractMetaStore> allFederatedMetastores = federatedMetaStoreStorage.getAll();
    LOG.info("allFederatedMetastores = {} and size = {}", allFederatedMetastores.toString(),
        allFederatedMetastores.size());
    return allFederatedMetastores;
  }

  public static interface FederationEventListener {

    void onRegister(AbstractMetaStore federatedMetaStore);

    void onUnregister(AbstractMetaStore federatedMetaStore);

    void onUpdate(AbstractMetaStore oldMetaStore, AbstractMetaStore newMetaStore);
  }
}
