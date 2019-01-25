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

import com.google.common.collect.Sets;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMapConfig;
import io.atomix.core.map.AtomicMapType;
import io.atomix.rest.AtomixResource;
import io.atomix.utils.time.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Atomic map resource.
 */
@AtomixResource
@Path("/atomic-map")
public class AtomicMapResource extends PrimitiveResource<AsyncAtomicMap<String, String>, AtomicMapConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AtomicMapResource.class);

  public AtomicMapResource() {
    super(AtomicMapType.instance());
  }

  @GET
  @Path("/{name}/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  public void get(
      @PathParam("name") String name,
      @PathParam("key") String key,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(map -> map.get(key)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result != null ? new VersionedResult(result) : null).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @PUT
  @Path("/{name}/{key}")
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.APPLICATION_JSON)
  public void put(
      @PathParam("name") String name,
      @PathParam("key") String key,
      String value,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(map -> map.put(key, value)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result != null ? new VersionedResult(result) : null).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @DELETE
  @Path("/{name}/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  public void remove(
      @PathParam("name") String name,
      @PathParam("key") String key,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(map -> map.remove(key)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result != null ? new VersionedResult(result) : null).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @GET
  @Path("/{name}/keys")
  @Produces(MediaType.APPLICATION_JSON)
  public void keys(
      @PathParam("name") String name,
      @Suspended AsyncResponse response) {
    getPrimitive(name).whenComplete((map, error) -> {
      if (error == null) {
        response.resume(Response.ok(Sets.newHashSet(map.keySet().iterator().sync())).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @GET
  @Path("/{name}/size")
  @Produces(MediaType.APPLICATION_JSON)
  public void size(
      @PathParam("name") String name,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(map -> map.size()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @POST
  @Path("/{name}/clear")
  public void clear(
      @PathParam("name") String name,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(map -> map.clear()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.noContent().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  /**
   * Versioned JSON result.
   */
  static class VersionedResult {
    private final Versioned<String> value;

    VersionedResult(Versioned<String> value) {
      this.value = value;
    }

    public String getValue() {
      return value.value();
    }

    public long getVersion() {
      return value.version();
    }
  }
}
