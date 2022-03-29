// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest.resources;

import io.atomix.core.semaphore.AsyncDistributedSemaphore;
import io.atomix.core.semaphore.DistributedSemaphoreConfig;
import io.atomix.core.semaphore.DistributedSemaphoreType;
import io.atomix.rest.AtomixResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Distributed semaphore resource.
 */
@AtomixResource
@Path("/semaphore")
public class DistributedSemaphoreResource extends PrimitiveResource<AsyncDistributedSemaphore, DistributedSemaphoreConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedSemaphoreResource.class);

  public DistributedSemaphoreResource() {
    super(DistributedSemaphoreType.instance());
  }

  @POST
  @Path("/{name}/acquire")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void acquire(
      @PathParam("name") String name,
      Integer permits,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(semaphore -> semaphore.acquire(permits != null ? permits : 1)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("An error occurred", error);
        response.resume(Response.serverError());
      }
    });
  }

  @POST
  @Path("/{name}/release")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void release(
      @PathParam("name") String name,
      Integer permits,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(semaphore -> semaphore.release(permits != null ? permits : 1)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("An error occurred", error);
        response.resume(Response.serverError());
      }
    });
  }

  @POST
  @Path("/{name}/increase")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void increase(
      @PathParam("name") String name,
      Integer permits,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(semaphore -> semaphore.increasePermits(permits != null ? permits : 1)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("An error occurred", error);
        response.resume(Response.serverError());
      }
    });
  }

  @POST
  @Path("/{name}/reduce")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public void reduce(
      @PathParam("name") String name,
      Integer permits,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(semaphore -> semaphore.reducePermits(permits != null ? permits : 1)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("An error occurred", error);
        response.resume(Response.serverError());
      }
    });
  }

  @GET
  @Path("/{name}/permits")
  @Produces(MediaType.APPLICATION_JSON)
  public void availablePermits(
      @PathParam("name") String name,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(semaphore -> semaphore.availablePermits()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("An error occurred", error);
        response.resume(Response.serverError());
      }
    });
  }
}
