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

import io.atomix.core.queue.AsyncDistributedQueue;
import io.atomix.core.queue.DistributedQueueConfig;
import io.atomix.core.queue.DistributedQueueType;
import io.atomix.rest.AtomixResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Distributed queue resource.
 */
@AtomixResource
@Path("/queue")
public class DistributedQueueResource extends DistributedCollectionResource<AsyncDistributedQueue<String>, DistributedQueueConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedQueueResource.class);

  public DistributedQueueResource() {
    super(DistributedQueueType.instance());
  }

  @POST
  @Path("/{name}/remove")
  @Produces(MediaType.APPLICATION_JSON)
  public void remove(
      @PathParam("name") String name,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(queue -> queue.remove()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @POST
  @Path("/{name}/poll")
  @Produces(MediaType.APPLICATION_JSON)
  public void poll(
      @PathParam("name") String name,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(queue -> queue.poll()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @POST
  @Path("/{name}/element")
  @Produces(MediaType.APPLICATION_JSON)
  public void element(
      @PathParam("name") String name,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(queue -> queue.element()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @POST
  @Path("/{name}/peek")
  @Produces(MediaType.APPLICATION_JSON)
  public void peek(
      @PathParam("name") String name,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(queue -> queue.peek()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }
}
