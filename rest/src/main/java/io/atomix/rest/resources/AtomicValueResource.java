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

import io.atomix.core.value.AsyncAtomicValue;
import io.atomix.core.value.AtomicValueConfig;
import io.atomix.core.value.AtomicValueType;
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
 * Atomic value resource.
 */
@AtomixResource
@Path("/atomic-value")
public class AtomicValueResource extends PrimitiveResource<AsyncAtomicValue<String>, AtomicValueConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(AtomicValueResource.class);

  public AtomicValueResource() {
    super(AtomicValueType.instance());
  }

  @GET
  @Path("/{name}/value")
  @Produces(MediaType.APPLICATION_JSON)
  public void get(
      @PathParam("name") String name,
      @Suspended AsyncResponse response) {
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
  public void set(
      @PathParam("name") String name,
      String body,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(value -> value.set(body)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @POST
  @Path("/{name}/cas")
  @Produces(MediaType.APPLICATION_JSON)
  public void compareAndSet(
      @PathParam("name") String name,
      CompareAndSetRequest request,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(value -> value.compareAndSet(request.getExpect(), request.getUpdate())).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  /**
   * Compare and set request.
   */
  static class CompareAndSetRequest {
    private String expect;
    private String update;

    public String getExpect() {
      return expect;
    }

    public void setExpect(String expect) {
      this.expect = expect;
    }

    public String getUpdate() {
      return update;
    }

    public void setUpdate(String update) {
      this.update = update;
    }
  }
}
