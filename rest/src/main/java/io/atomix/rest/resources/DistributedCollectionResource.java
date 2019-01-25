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
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.collection.DistributedCollectionConfig;
import io.atomix.primitive.PrimitiveType;
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
 * Distributed collection resource.
 */
public abstract class DistributedCollectionResource<P extends AsyncDistributedCollection<String>, C extends DistributedCollectionConfig<C>> extends PrimitiveResource<P, C> {
  private final Logger log = LoggerFactory.getLogger(getClass());

  protected DistributedCollectionResource(PrimitiveType type) {
    super(type);
  }

  @GET
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public void get(
      @PathParam("name") String name,
      @Suspended AsyncResponse response) {
    getPrimitive(name).whenComplete((collection, error) -> {
      if (error == null) {
        response.resume(Response.ok(Sets.newHashSet(collection.iterator().sync())));
      } else {
        response.resume(Response.serverError().build());
      }
    });
  }

  @PUT
  @Path("/{name}/{element}")
  @Produces(MediaType.APPLICATION_JSON)
  public void add(
      @PathParam("name") String name,
      @PathParam("element") String element,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(collection -> collection.add(element)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        log.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @GET
  @Path("/{name}/{element}")
  @Produces(MediaType.APPLICATION_JSON)
  public void contains(
      @PathParam("name") String name,
      @PathParam("element") String element,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(collection -> collection.contains(element)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        log.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @DELETE
  @Path("/{name}/{element}")
  @Produces(MediaType.APPLICATION_JSON)
  public void remove(
      @PathParam("name") String name,
      @PathParam("element") String element,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(collection -> collection.remove(element)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        log.warn("{}", error);
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
    getPrimitive(name).thenCompose(collection -> collection.size()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        log.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @DELETE
  @Path("/{name}")
  public void clear(
      @PathParam("name") String name,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(collection -> collection.clear()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        log.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }
}
