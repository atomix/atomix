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

import io.atomix.core.map.AsyncConsistentMap;
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
 * Consistent map resource.
 */
public class ConsistentMapResource extends AbstractRestResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsistentMapResource.class);

  private final AsyncConsistentMap<String, String> map;

  public ConsistentMapResource(AsyncConsistentMap<String, String> map) {
    this.map = map;
  }

  @GET
  @Path("/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  public void get(@PathParam("key") String key, @Suspended AsyncResponse response) {
    map.get(key).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result != null ? new VersionedResult(result) : null).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @PUT
  @Path("/{key}")
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.APPLICATION_JSON)
  public void put(@PathParam("key") String key, String value, @Suspended AsyncResponse response) {
    map.put(key, value).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result != null ? new VersionedResult(result) : null).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @DELETE
  @Path("/{key}")
  @Produces(MediaType.APPLICATION_JSON)
  public void remove(@PathParam("key") String key, @Suspended AsyncResponse response) {
    map.remove(key).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result != null ? new VersionedResult(result) : null).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @GET
  @Path("/keys")
  @Produces(MediaType.APPLICATION_JSON)
  public void keys(@Suspended AsyncResponse response) {
    map.keySet().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @GET
  @Path("/size")
  @Produces(MediaType.APPLICATION_JSON)
  public void size(@Suspended AsyncResponse response) {
    map.size().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @POST
  @Path("/clear")
  public void clear(@Suspended AsyncResponse response) {
    map.clear().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.noContent().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @DELETE
  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public void delete(@Suspended AsyncResponse response) {
    clear(response);
  }

  /**
   * Versioned JSON result.
   */
  static class VersionedResult {
    private final Versioned<String> value;

    public VersionedResult(Versioned<String> value) {
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
