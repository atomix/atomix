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

import io.atomix.core.lock.AsyncAtomicLock;
import io.atomix.core.lock.AtomicLockConfig;
import io.atomix.core.lock.AtomicLockType;
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
@Path("/atomic-lock")
public class AtomicLockResource extends PrimitiveResource<AsyncAtomicLock, AtomicLockConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AtomicLockResource.class);

  public AtomicLockResource() {
    super(AtomicLockType.instance());
  }

  @POST
  @Path("/{name}/lock")
  @Produces(MediaType.APPLICATION_JSON)
  public void lock(@PathParam("name") String name, @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(lock -> lock.lock()).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result.value()).build());
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
