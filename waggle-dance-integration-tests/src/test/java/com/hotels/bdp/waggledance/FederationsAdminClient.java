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
package com.hotels.bdp.waggledance;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

import com.hotels.bdp.waggledance.api.model.AbstractMetaStore;

public interface FederationsAdminClient {

  @GET
  @Path("/api/admin/federations")
  List<AbstractMetaStore> federations();

  @GET
  @Path("/api/admin/federations/{name}")
  AbstractMetaStore read(@PathParam("name") String name);

  @POST
  @Path("/api/admin/federations")
  @Consumes(MediaType.APPLICATION_JSON)
  void add(AbstractMetaStore federatedMetaStore);

  @DELETE
  @Path("/api/admin/federations/{name}")
  void remove(String name);

}
