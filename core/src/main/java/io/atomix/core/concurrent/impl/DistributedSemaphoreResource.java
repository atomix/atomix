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
package io.atomix.core.concurrent.impl;

import io.atomix.core.concurrent.AsyncDistributedSemaphore;
import io.atomix.primitive.resource.PrimitiveResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Distributed semaphore resource.
 */
public class DistributedSemaphoreResource implements PrimitiveResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedSemaphoreResource.class);
  private final AsyncDistributedSemaphore semaphore;

  public DistributedSemaphoreResource(AsyncDistributedSemaphore semaphore) {
    this.semaphore = semaphore;
  }

  @POST
  @Path("/acquire")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void acquire(Integer permits, @Suspended AsyncResponse response) {
    semaphore.acquire(permits != null ? permits : 1).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result.value()).build());
      } else {
        LOGGER.warn("An error occurred", error);
        response.resume(Response.serverError());
      }
    });
  }

  @POST
  @Path("/release")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void release(Integer permits, @Suspended AsyncResponse response) {
    semaphore.release(permits != null ? permits : 1).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("An error occurred", error);
        response.resume(Response.serverError());
      }
    });
  }

  @POST
  @Path("/increase")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void increase(Integer permits, @Suspended AsyncResponse response) {
    semaphore.increase(permits != null ? permits : 1).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("An error occurred", error);
        response.resume(Response.serverError());
      }
    });
  }

  @POST
  @Path("/reduce")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void reduce(Integer permits, @Suspended AsyncResponse response) {
    semaphore.reduce(permits != null ? permits : 1).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("An error occurred", error);
        response.resume(Response.serverError());
      }
    });
  }

  @GET
  @Path("/permits")
  @Produces(MediaType.APPLICATION_JSON)
  public void availablePermits(@Suspended AsyncResponse response) {
    semaphore.availablePermits().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("An error occurred", error);
        response.resume(Response.serverError());
      }
    });
  }
}
