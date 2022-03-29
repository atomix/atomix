// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.rest.resources;

import io.atomix.core.idgenerator.AsyncAtomicIdGenerator;
import io.atomix.core.idgenerator.AtomicIdGeneratorConfig;
import io.atomix.core.idgenerator.AtomicIdGeneratorType;
import io.atomix.rest.AtomixResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Unique ID generator resource.
 */
@AtomixResource
@Path("/atomic-id-generator")
public class AtomicIdGeneratorResource extends PrimitiveResource<AsyncAtomicIdGenerator, AtomicIdGeneratorConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AtomicIdGeneratorResource.class);

  public AtomicIdGeneratorResource() {
    super(AtomicIdGeneratorType.instance());
  }

  @PUT
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public void next(
      @PathParam("name") String name,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(idGenerator -> idGenerator.nextId()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }
}
