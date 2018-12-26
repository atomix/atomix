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
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.Subscription;
import io.atomix.rest.AtomixResource;
import io.atomix.rest.impl.EventLog;
import io.atomix.rest.impl.EventManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Events resource.
 */
@AtomixResource
@Path("/events")
public class EventsResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventsResource.class);
  private static final int UUID_STRING_LENGTH = UUID.randomUUID().toString().length();

  /**
   * Returns an event log name.
   */
  private String getEventLogName(String subject, String id) {
    return String.format("%s-%s", subject, id);
  }

  @POST
  @Path("/{subject}")
  @Consumes(MediaType.TEXT_PLAIN)
  public Response publish(@PathParam("subject") String subject, @Context ClusterEventService eventService, String body) {
    eventService.broadcast(subject, body);
    return Response.ok().build();
  }

  @GET
  @Path("/{subject}")
  @Produces(MediaType.TEXT_PLAIN)
  public void next(@PathParam("subject") String subject, @Context ClusterEventService eventService, @Context EventManager events, @Suspended AsyncResponse response) {
    EventLog<Consumer<String>, String> eventLog = events.getOrCreateEventLog(
        ClusterEventService.class, subject, l -> e -> l.addEvent(e));
    CompletableFuture<Subscription> openFuture;
    if (eventLog.open()) {
      openFuture = eventService.subscribe(subject, eventLog.listener(), MoreExecutors.directExecutor());
    } else {
      openFuture = CompletableFuture.completedFuture(null);
    }

    openFuture.whenComplete((result, error) -> {
      if (error == null) {
        eventLog.nextEvent().whenComplete((event, eventError) -> {
          if (eventError == null) {
            response.resume(Response.ok(event).build());
          } else {
            response.resume(Response.noContent().build());
          }
        });
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @DELETE
  @Path("/{subject}")
  public void delete(@PathParam("subject") String subject, @Context ClusterEventService eventService, @Context EventManager events, @Suspended AsyncResponse response) {
    EventLog<Consumer<String>, String> eventLog = events.removeEventLog(ClusterEventService.class, subject);
    if (eventLog != null && eventLog.close()) {
      List<Subscription> subscriptions = eventService.getSubscriptions(subject);
      if (subscriptions != null && !subscriptions.isEmpty()) {
        subscriptions.get(0).close().whenComplete((result, error) -> {
          response.resume(Response.ok().build());
        });
      }
    }
  }

  @GET
  @Path("/{subject}/subscribers")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getSubscribers(@PathParam("subject") String subject, @Context EventManager events) {
    return Response.ok(events.getEventLogNames(ClusterEventService.class)
        .stream()
        .filter(name -> name.length() == subject.length() + 1 + UUID_STRING_LENGTH && name.substring(0, name.length() - UUID_STRING_LENGTH - 1).equals(subject))
        .map(name -> name.substring(subject.length() + 1))
        .collect(Collectors.toList())).build();
  }

  @POST
  @Path("/{subject}/subscribers")
  @Produces(MediaType.TEXT_PLAIN)
  public void subscribe(@PathParam("subject") String subject, @Context ClusterEventService eventService, @Context EventManager events, @Suspended AsyncResponse response) {
    String id = UUID.randomUUID().toString();
    EventLog<Consumer<String>, String> eventLog = events.getOrCreateEventLog(
        ClusterEventService.class, getEventLogName(subject, id), l -> e -> l.addEvent(e));
    eventService.subscribe(subject, eventLog.listener(), MoreExecutors.directExecutor())
        .whenComplete((result, error) -> {
          if (error == null) {
            response.resume(Response.ok(id).build());
          } else {
            LOGGER.warn("{}", error);
            response.resume(Response.serverError().build());
          }
        });
  }

  @GET
  @Path("/{subject}/subscribers/{id}")
  @Produces(MediaType.TEXT_PLAIN)
  public void nextSession(@PathParam("subject") String subject, @PathParam("id") String id, @Context EventManager events, @Suspended AsyncResponse response) {
    EventLog<Consumer<String>, String> eventLog = events.getEventLog(ClusterEventService.class, getEventLogName(subject, id));
    if (eventLog == null) {
      LOGGER.warn("Unknown subscriber {}", id);
      response.resume(Response.status(Status.NOT_FOUND).build());
      return;
    }

    eventLog.nextEvent().whenComplete((event, error) -> {
      if (error == null) {
        response.resume(Response.ok(event).build());
      } else {
        LOGGER.warn("Subscriber {} closed", id);
        response.resume(Response.noContent().build());
      }
    });
  }

  @DELETE
  @Path("/{subject}/subscribers/{id}")
  public void unsubscribe(@PathParam("subject") String subject, @PathParam("id") String id, @Context ClusterEventService eventService, @Context EventManager events, @Suspended AsyncResponse response) {
    EventLog<Consumer<String>, String> eventLog = events.getEventLog(ClusterEventService.class, getEventLogName(subject, id));
    if (eventLog != null && eventLog.close()) {
      List<Subscription> subscriptions = eventService.getSubscriptions(subject);
      if (subscriptions != null && !subscriptions.isEmpty()) {
        // TODO: This is not closing the correct subscription
        subscriptions.get(0).close().whenComplete((result, error) -> {
          response.resume(Response.ok().build());
        });
      }
    }
  }
}
