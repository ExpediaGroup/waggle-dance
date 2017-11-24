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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.commons.pool2.KeyedObjectPool;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.hotels.bdp.waggledance.api.WaggleDanceException;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.client.CloseableThriftHiveMetastoreIface;

@RunWith(MockitoJUnitRunner.class)
public class MetaStoreClientPoolTest {

  private @Mock KeyedObjectPool<AbstractMetaStore, CloseableThriftHiveMetastoreIface> delegate;
  private @Mock AbstractMetaStore key;
  private @Mock CloseableThriftHiveMetastoreIface client;

  private MetaStoreClientPool pool;

  @Before
  public void setUp() {
    pool = new MetaStoreClientPool(delegate);
  }

  @Test
  public void borrowObject() throws Exception {
    when(delegate.borrowObject(key)).thenReturn(client);
    assertThat(pool.borrowObject(key), is(client));
  }

  @Test
  public void borrowObjectUnchecked() throws Exception {
    when(delegate.borrowObject(key)).thenReturn(client);
    assertThat(pool.borrowObjectUnchecked(key), is(client));
  }

  @Test(expected = WaggleDanceException.class)
  public void borrowObjectUncheckedWrapException() throws Exception {
    when(delegate.borrowObject(key)).thenThrow(new Exception());
    pool.borrowObjectUnchecked(key);
  }

  @Test
  public void returnObject() throws Exception {
    pool.returnObject(key, client);
    verify(delegate).returnObject(key, client);
  }

  @Test
  public void returnObjectUnchecked() throws Exception {
    pool.returnObjectUnchecked(key, client);
    verify(delegate).returnObject(key, client);
  }

  @Test
  public void returnObjectUncheckedIgnoreException() throws Exception {
    pool.returnObjectUnchecked(key, client);
    doThrow(new Exception()).when(delegate).returnObject(key, client);
  }

  public void invalidateObject() throws Exception {
    pool.invalidateObject(key, client);
    verify(delegate).invalidateObject(key, client);
  }

  @Test
  public void addObject() throws Exception {
    pool.addObject(key);
    verify(delegate).addObject(key);
  }

  @Test
  public void getNumIdleKey() {
    when(delegate.getNumIdle(key)).thenReturn(3);
    assertThat(pool.getNumIdle(key), is(3));
  }

  @Test
  public void getNumActiveKey() {
    when(delegate.getNumActive(key)).thenReturn(3);
    assertThat(pool.getNumActive(key), is(3));
  }

  @Test
  public void getNumIdle() {
    when(delegate.getNumIdle()).thenReturn(3);
    assertThat(pool.getNumIdle(), is(3));
  }

  @Test
  public void getNumActive() {
    when(delegate.getNumActive()).thenReturn(3);
    assertThat(pool.getNumActive(), is(3));
  }

  @Test
  public void clear() throws Exception {
    pool.clear();
    verify(delegate).clear();
  }

  @Test
  public void clearKey() throws Exception {
    pool.clear(key);
    verify(delegate).clear(key);
  }

  @Test
  public void close() {
    pool.close();
    verify(delegate).close();
  }
}
