// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest.resources;

import io.atomix.core.lock.AsyncDistributedLock;
import io.atomix.core.lock.DistributedLockConfig;
import io.atomix.core.lock.DistributedLockType;
import io.atomix.rest.AtomixResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Distributed lock resource.
 */
@AtomixResource
@Path("/lock")
public class DistributedLockResource extends PrimitiveResource<AsyncDistributedLock, DistributedLockConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedLockResource.class);

  public DistributedLockResource() {
    super(DistributedLockType.instance());
  }

  @POST
  @Path("/{name}/lock")
  @Produces(MediaType.APPLICATION_JSON)
  public void lock(@PathParam("name") String name, @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(lock -> lock.lock()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @DELETE
  @Path("/{name}/lock")
  public void unlock(@PathParam("name") String name, @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(lock -> lock.unlock()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }
}
