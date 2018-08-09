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

import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.core.workqueue.AsyncWorkQueue;
import io.atomix.core.workqueue.WorkQueueConfig;
import io.atomix.core.workqueue.WorkQueueType;
import io.atomix.rest.AtomixResource;
import io.atomix.rest.impl.EventLog;
import io.atomix.rest.impl.EventManager;
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
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.function.Consumer;

/**
 * Work queue resource.
 */
@AtomixResource
@Path("/work-queue")
public class WorkQueueResource extends PrimitiveResource<AsyncWorkQueue<String>, WorkQueueConfig> {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkQueueResource.class);

  public WorkQueueResource() {
    super(WorkQueueType.instance());
  }

  @POST
  @Path("/{name}")
  @Consumes(MediaType.TEXT_PLAIN)
  public void add(
      @PathParam("name") String name,
      String item,
      @Suspended AsyncResponse response) {
    getPrimitive(name).thenCompose(queue -> queue.addOne(item)).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @GET
  @Path("/{name}")
  @Produces(MediaType.APPLICATION_JSON)
  public void take(
      @PathParam("name") String name,
      @Context EventManager events,
      @Suspended AsyncResponse response) {
    EventLog<Consumer<String>, String> eventLog = events.getOrCreateEventLog(
        AsyncWorkQueue.class, name, l -> e -> l.addEvent(e));
    if (eventLog.open()) {
      getPrimitive(name).thenCompose(queue -> queue.registerTaskProcessor(eventLog.listener(), 1, MoreExecutors.directExecutor()))
          .whenComplete((result, error) -> {
            if (error == null) {
              takeTask(eventLog, response);
            } else {
              LOGGER.warn("{}", error);
              response.resume(Response.serverError().build());
            }
          });
    } else {
      takeTask(eventLog, response);
    }
  }

  private void takeTask(EventLog<Consumer<String>, String> eventLog, AsyncResponse response) {
    eventLog.nextEvent().whenComplete((eventResult, eventError) -> {
      if (eventError == null) {
        response.resume(Response.ok(eventResult).build());
      } else {
        response.resume(Response.status(Status.NOT_FOUND).build());
      }
    });
  }
}
