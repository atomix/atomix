// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest.resources;

import io.atomix.core.value.AsyncDistributedValue;
import io.atomix.core.value.DistributedValueConfig;
import io.atomix.core.value.DistributedValueType;
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
 * Distributed value resource.
 */
@AtomixResource
@Path("/value")
public class DistributedValueResource extends PrimitiveResource<AsyncDistributedValue<String>, DistributedValueConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedValueResource.class);

  public DistributedValueResource() {
    super(DistributedValueType.instance());
  }

  @GET
  @Path("/{name}/value")
  @Produces(MediaType.APPLICATION_JSON)
  public void get(@PathParam("name") String name, @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(value -> value.get()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @POST
  @Path("/{name}/value")
  @Consumes(MediaType.TEXT_PLAIN)
  public void set(@PathParam("name") String name, String body, @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(value -> value.set(body)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }
}
