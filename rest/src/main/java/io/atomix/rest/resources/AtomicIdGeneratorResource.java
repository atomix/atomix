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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.core.generator.AsyncAtomicIdGenerator;

import javax.ws.rs.PUT;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Unique ID generator resource.
 */
public class AtomicIdGeneratorResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(AtomicIdGeneratorResource.class);

  private final AsyncAtomicIdGenerator idGenerator;

  public AtomicIdGeneratorResource(AsyncAtomicIdGenerator idGenerator) {
    this.idGenerator = idGenerator;
  }

  @PUT
  @Produces(MediaType.APPLICATION_JSON)
  public void next(@Suspended AsyncResponse response) {
    idGenerator.nextId().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }
}
