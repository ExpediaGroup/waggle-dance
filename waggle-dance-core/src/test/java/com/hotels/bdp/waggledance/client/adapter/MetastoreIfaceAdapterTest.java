/**
 * Copyright (C) 2016-2025 Expedia, Inc.
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
package com.hotels.bdp.waggledance.client.adapter;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import com.facebook.fb303.fb_status;

/**
 * Fully are this doesn't comes even close to full test coverage. Lots of methods are rather trivial and fairly useless
 * to add, just add when needed. Some basic methods have been added that are needed as a start.
 */
@RunWith(MockitoJUnitRunner.class)
public class MetastoreIfaceAdapterTest {

  private @Mock IMetaStoreClient client;
  private MetastoreIfaceAdapter adapter;

  @Before
  public void setUp() {
    adapter = new MetastoreIfaceAdapter(client);
  }

  @Test
  public void getStatus() throws Exception {
    assertThat(adapter.getStatus(), is(fb_status.ALIVE));
  }

  @Test
  public void getStatusDetails() throws Exception {
    assertThat(adapter.getStatusDetails(), is(fb_status.ALIVE.toString()));
  }

  @Test
  public void reinitialize() throws Exception {
    adapter.reinitialize();
    verify(client).reconnect();
  }

  @Test
  public void shutdown() throws Exception {
    adapter.shutdown();
    verify(client).close();
  }

  @Test
  public void close() throws Exception {
    adapter.close();
    verify(client).close();
  }

  @Test
  public void isOpen() throws Exception {
    assertThat(adapter.isOpen(), is(true));
  }
}
