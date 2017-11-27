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
package com.hotels.bdp.waggledance.client.pool;

import java.util.NoSuchElementException;

import org.apache.commons.pool2.KeyedObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;

/**
 * Simple delegate to get rid of generics and add unchecked {@link #borrowObjectUnchecked(AbstractMetaStore)},
 * {@link #returnObjectUnchecked(AbstractMetaStore, CloseableThriftHiveMetastoreIface) methods.
 */
public class MetaStoreClientPool implements KeyedObjectPool<AbstractMetaStore, CloseableThriftHiveMetastoreIface> {

  private final static Logger LOG = LoggerFactory.getLogger(MetaStoreClientPool.class);

  private final KeyedObjectPool<AbstractMetaStore, CloseableThriftHiveMetastoreIface> pool;

  public MetaStoreClientPool(KeyedObjectPool<AbstractMetaStore, CloseableThriftHiveMetastoreIface> pool) {
    this.pool = pool;
  }

  @Override
  public CloseableThriftHiveMetastoreIface borrowObject(AbstractMetaStore key)
    throws Exception, NoSuchElementException, IllegalStateException {
    return pool.borrowObject(key);
  }

  @Override
  public void returnObject(AbstractMetaStore key, CloseableThriftHiveMetastoreIface obj) throws Exception {
    pool.returnObject(key, obj);
  }

  @Override
  public void invalidateObject(AbstractMetaStore key, CloseableThriftHiveMetastoreIface obj) throws Exception {
    obj.close();
    pool.invalidateObject(key, obj);
  }

  @Override
  public void addObject(AbstractMetaStore key) throws Exception, IllegalStateException, UnsupportedOperationException {
    pool.addObject(key);
  }

  @Override
  public int getNumIdle(AbstractMetaStore key) {
    return pool.getNumIdle(key);
  }

  @Override
  public int getNumActive(AbstractMetaStore key) {
    return pool.getNumActive(key);
  }

  @Override
  public int getNumIdle() {
    return pool.getNumIdle();
  }

  @Override
  public int getNumActive() {
    return pool.getNumActive();
  }

  @Override
  public void clear() throws Exception, UnsupportedOperationException {
    pool.clear();
  }

  @Override
  public void clear(AbstractMetaStore key) throws Exception, UnsupportedOperationException {
    pool.clear(key);
  }

  @Override
  public void close() {
    pool.close();
  }

  public void returnObjectUnchecked(AbstractMetaStore metaStore, CloseableThriftHiveMetastoreIface client) {
    LOG.debug("returning client to pool for key: {}, activeCount (total): {} ({}), idleCount (total): {} ({})",
        metaStore.getName(), getNumActive(metaStore), getNumActive(), getNumIdle(metaStore), getNumIdle());
    try {
      pool.returnObject(metaStore, client);
    } catch (Exception e) {
      LOG.warn("Could not return object to the pool, metastore is '" + metaStore.getName() + "'", e);
    }

  }

  public CloseableThriftHiveMetastoreIface borrowObjectUnchecked(AbstractMetaStore metaStore) {
    LOG.debug("borrow client to pool for key: {}, activeCount (total): {} ({}), idleCount (total): {} ({})",
        metaStore.getName(), getNumActive(metaStore), getNumActive(), getNumIdle(metaStore), getNumIdle());
    try {
      return pool.borrowObject(metaStore);
    } catch (Exception e) {
      throw new WaggleDanceException("Cannot get a client from the pool", e);
    }
  }

}
