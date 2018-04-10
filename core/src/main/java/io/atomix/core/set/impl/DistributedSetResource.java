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
package io.atomix.core.set.impl;

import io.atomix.core.set.DistributedSet;
import io.atomix.primitive.resource.PrimitiveResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Distributed set resource.
 */
public class DistributedSetResource extends PrimitiveResource<DistributedSet<String>> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedSetResource.class);

  public DistributedSetResource(DistributedSet<String> set) {
    super(set);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public void get(@Suspended AsyncResponse response) {
    primitive.async().getAsImmutableSet().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @PUT
  @Path("/{element}")
  @Produces(MediaType.APPLICATION_JSON)
  public void add(@PathParam("element") String element, @Suspended AsyncResponse response) {
    primitive.async().add(element).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @GET
  @Path("/{element}")
  @Produces(MediaType.APPLICATION_JSON)
  public void contains(@PathParam("element") String element, @Suspended AsyncResponse response) {
    primitive.async().contains(element).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @DELETE
  @Path("/{element}")
  @Produces(MediaType.APPLICATION_JSON)
  public void remove(@PathParam("element") String element, @Suspended AsyncResponse response) {
    primitive.async().remove(element).whenComplete((result, error) -> {
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
    primitive.async().size().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @DELETE
  public void clear(@Suspended AsyncResponse response) {
    primitive.async().clear().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }
}
