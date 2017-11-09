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
package io.atomix.rest.impl;

import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.primitives.queue.AsyncWorkQueue;
import io.atomix.primitives.queue.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Work queue resource.
 */
public class WorkQueueResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(WorkQueueResource.class);

  private final AsyncWorkQueue<String> workQueue;

  public WorkQueueResource(AsyncWorkQueue<String> workQueue) {
    this.workQueue = workQueue;
  }

  @PUT
  @Consumes(MediaType.TEXT_PLAIN)
  public void add(String item, @Suspended AsyncResponse response) {
    workQueue.addOne(item).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public void take(@QueryParam("items") Integer items, @Context EventManager events, @Suspended AsyncResponse response) {
    EventLog<Consumer<String>, String> eventLog = events.getOrCreateEventLog(
        AsyncWorkQueue.class, workQueue.name(), l -> e -> l.addEvent(e));
    if (eventLog.open()) {
      workQueue.registerTaskProcessor(eventLog.listener(), 1, MoreExecutors.directExecutor())
          .whenComplete((result, error) -> {
            if (error == null) {
              takeTasks(eventLog, items, response);
            } else {
              LOGGER.warn("{}", error);
              response.resume(Response.serverError().build());
            }
          });
    } else {
      takeTasks(eventLog, items, response);
    }
  }

  private void takeTasks(EventLog<Consumer<String>, String> eventLog, Integer items, AsyncResponse response) {
    eventLog.nextEvent().whenComplete((eventResult, eventError) -> {
      if (eventError == null) {
        workQueue.take(items != null ? items : 1).whenComplete((takeResult, takeError) -> {
          if (takeError == null) {
            if (!takeResult.isEmpty()) {
              response.resume(Response.ok(takeResult.stream()
                  .map(TaskResponse::new)
                  .collect(Collectors.toList()))
                  .build());
            } else {
              takeTasks(eventLog, items, response);
            }
          } else {
            LOGGER.warn("{}", takeError);
            response.resume(Response.serverError().build());
          }
        });
      } else {
        response.resume(Response.status(Status.NOT_FOUND).build());
      }
    });
  }

  @DELETE
  @Consumes(MediaType.APPLICATION_JSON)
  public void completeTasks(List<String> taskIds, @Suspended AsyncResponse response) {
    workQueue.complete(taskIds).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @DELETE
  @Path("/{taskId}")
  public void completeTask(@PathParam("taskId") String taskId, @Suspended AsyncResponse response) {
    workQueue.complete(taskId).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  /**
   * Task response.
   */
  static class TaskResponse {
    private final Task<String> task;

    public TaskResponse(Task<String> task) {
      this.task = task;
    }

    public String getId() {
      return task.taskId();
    }

    public String getPayload() {
      return task.payload();
    }
  }
}
