/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.rest.resources;

import io.atomix.core.PrimitivesService;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.PrimitiveConfig;
import io.atomix.primitive.resource.PrimitiveResource;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Primitives resource.
 */
@Path("/v1/primitives")
public class PrimitivesResource extends AbstractRestResource {

  /**
   * Returns a primitive resource by name.
   */
  @Path("/{name}")
  @SuppressWarnings("unchecked")
  public PrimitiveResource getPrimitive(@PathParam("name") String name, @Context PrimitivesService primitives) {
    DistributedPrimitive primitive = primitives.getPrimitive(name);
    return (PrimitiveResource) primitive.primitiveType().resourceFactory().apply(primitive);
  }

  /**
   * Creates a new primitive resource.
   */
  @POST
  @Path("/{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @SuppressWarnings("unchecked")
  public Response createPrimitive(@PathParam("name") String name, PrimitiveConfig config, @Context PrimitivesService primitives) {
    try {
      primitives.getPrimitive(name, config.getType(), config);
      return Response.ok().build();
    } catch (Exception e) {
      return Response.status(Response.Status.NOT_ACCEPTABLE).build();
    }
  }
}
