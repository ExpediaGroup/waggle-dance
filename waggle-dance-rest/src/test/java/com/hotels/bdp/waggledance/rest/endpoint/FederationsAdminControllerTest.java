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
package com.hotels.bdp.waggledance.rest.endpoint;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import jersey.repackaged.com.google.common.collect.Lists;

import com.hotels.bdp.waggledance.api.federation.service.FederationService;
import com.hotels.bdp.waggledance.api.federation.service.FederationStatusService;
import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;
import com.hotels.bdp.waggledance.api.model.AccessControlType;
import com.hotels.bdp.waggledance.api.model.MetaStoreStatus;
import com.hotels.bdp.waggledance.api.model.PrimaryMetaStore;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { TestContext.class })
@WebAppConfiguration
public class FederationsAdminControllerTest {

  @Autowired
  private FederationService federationService;
  @Autowired
  private FederationStatusService federationStatusService;
  @Autowired
  private WebApplicationContext webApplicationContext;

  private MockMvc mockMvc;

  private final AbstractMetaStore metastore = new PrimaryMetaStore("primary", "uri",
      AccessControlType.READ_AND_WRITE_AND_CREATE);

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    when(federationStatusService.checkStatus(anyString())).thenReturn(MetaStoreStatus.AVAILABLE);
    metastore.setStatus(MetaStoreStatus.AVAILABLE);
  }

  @Test
  public void getAll() throws Exception {
    when(federationService.getAll()).thenReturn(Lists.newArrayList(metastore));

    String expected = Jackson2ObjectMapperBuilder
        .json()
        .build()
        .writeValueAsString(new AbstractMetaStore[] { metastore });
    mockMvc.perform(get("/api/admin/federations/")).andExpect(status().isOk()).andExpect(content().json(expected));
  }

  @Test
  public void getOnName() throws Exception {
    when(federationService.get("primary")).thenReturn(metastore);

    String expected = Jackson2ObjectMapperBuilder.json().build().writeValueAsString(metastore);
    mockMvc
        .perform(get("/api/admin/federations/primary"))
        .andExpect(status().isOk())
        .andExpect(content().json(expected));
  }

  @Test
  public void add() throws Exception {
    String content = Jackson2ObjectMapperBuilder.json().build().writeValueAsString(metastore);
    mockMvc
        .perform(post("/api/admin/federations/").contentType(MediaType.APPLICATION_JSON_UTF8).content(content))
        .andExpect(status().isOk());
    verify(federationService).register(metastore);
  }

  @Test
  public void deleteOnName() throws Exception {
    mockMvc.perform(delete("/api/admin/federations/primary")).andExpect(status().isOk());
    verify(federationService).unregister("primary");
  }

}
