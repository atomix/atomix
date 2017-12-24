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

import io.atomix.core.lock.AsyncDistributedLock;

import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Distributed lock resource.
 */
public class DistributedLockResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedLockResource.class);

  private final AsyncDistributedLock lock;

  public DistributedLockResource(AsyncDistributedLock lock) {
    this.lock = lock;
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public void lock(@Suspended AsyncResponse response) {
    lock.lock().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result.value()).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @DELETE
  public void unlock(@Suspended AsyncResponse response) {
    lock.unlock().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }
}
