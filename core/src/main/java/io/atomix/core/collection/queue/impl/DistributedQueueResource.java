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
package io.atomix.core.collection.queue.impl;

import io.atomix.core.collection.impl.DistributedCollectionResource;
import io.atomix.core.collection.queue.AsyncDistributedQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Distributed queue resource.
 */
public class DistributedQueueResource extends DistributedCollectionResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(DistributedQueueResource.class);

  private final AsyncDistributedQueue<String> queue;

  public DistributedQueueResource(AsyncDistributedQueue<String> queue) {
    super(queue);
    this.queue = queue;
  }

  @POST
  @Path("/remove")
  @Produces(MediaType.APPLICATION_JSON)
  public void remove(@Suspended AsyncResponse response) {
    queue.remove().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @POST
  @Path("/poll")
  @Produces(MediaType.APPLICATION_JSON)
  public void poll(@Suspended AsyncResponse response) {
    queue.poll().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @POST
  @Path("/element")
  @Produces(MediaType.APPLICATION_JSON)
  public void element(@Suspended AsyncResponse response) {
    queue.element().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @POST
  @Path("/peek")
  @Produces(MediaType.APPLICATION_JSON)
  public void peek(@Suspended AsyncResponse response) {
    queue.peek().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(result).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }
}
