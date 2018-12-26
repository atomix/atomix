/*
 * Copyright 2018-present Open Networking Foundation
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

import com.google.common.collect.Maps;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.PrimitiveFactory;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.SyncPrimitive;
import io.atomix.primitive.config.PrimitiveConfig;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Primitive resource.
 */
public abstract class PrimitiveResource<P extends AsyncPrimitive, C extends PrimitiveConfig<C>> {
  protected final PrimitiveType type;

  @Context
  protected PrimitiveFactory primitives;

  protected PrimitiveResource(PrimitiveType type) {
    this.type = type;
  }

  @SuppressWarnings("unchecked")
  protected CompletableFuture<P> getPrimitive(String name) {
    return primitives.getPrimitiveAsync(name, type).thenApply(primitive -> ((SyncPrimitive) primitive).async());
  }

  @GET
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPrimitives() {
    Map<String, PrimitiveInfo> primitivesInfo = primitives.getPrimitives(type)
        .stream()
        .map(info -> Maps.immutableEntry(info.name(), new PrimitiveInfo(info.name(), info.type().name())))
        .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
    return Response.ok(primitivesInfo).build();
  }

  @POST
  @Path("{name}")
  @Consumes(MediaType.APPLICATION_JSON)
  @SuppressWarnings("unchecked")
  public void create(
      @PathParam("name") String name,
      C config,
      @Suspended AsyncResponse response) {
    primitives.getPrimitiveAsync(name, type, config).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        response.resume(error);
      }
    });
  }

  static class PrimitiveInfo {
    private String name;
    private String type;

    PrimitiveInfo(String name, String type) {
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }
  }
}
