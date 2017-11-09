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
import io.atomix.cluster.messaging.ClusterEventService;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespaces;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Events resource.
 */
public class EventsResource {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespaces.BASIC);

  private final ClusterEventService eventService;
  private final Map<String, EventSubject> consumers = new ConcurrentHashMap<>();

  public EventsResource(ClusterEventService eventService) {
    this.eventService = eventService;
  }

  @POST
  @Path("/{subject}")
  public Response publish(@PathParam("subject") String subject, String body) {
    eventService.broadcast(body, new MessageSubject(subject), SERIALIZER::encode);
    return Response.ok().build();
  }

  @POST
  @Path("/{subject}/subscribe")
  public void subscribe(@PathParam("subject") String subject, @Suspended AsyncResponse response) {
    EventSubject consumer = new EventSubject();
    EventSubject existingConsumer = consumers.putIfAbsent(subject, consumer);
    if (existingConsumer == null) {
      eventService.addSubscriber(new MessageSubject(subject), SERIALIZER::decode, consumer, MoreExecutors.directExecutor())
          .whenComplete((result, error) -> {
            if (error == null) {
              response.resume(Response.ok(consumer.newSession()).build());
            } else {
              response.resume(Response.serverError().build());
            }
          });
    } else {
      response.resume(Response.ok(existingConsumer.newSession()).build());
    }
  }

  @GET
  @Path("/{subject}")
  public void next(@PathParam("subject") String subject, @QueryParam("session") Integer sessionId, @Suspended AsyncResponse response) {
    EventSubject consumer = consumers.get(subject);
    if (consumer == null) {
      response.resume(Response.noContent().build());
      return;
    }

    if (sessionId == null) {
      String event = consumer.nextEvent();
      if (event != null) {
        response.resume(Response.ok(event).build());
      } else {
        consumer.registerResponse(response);
      }
    } else {
      EventSession session = consumer.getSession(sessionId);
      if (session == null) {
        response.resume(Response.status(Status.NOT_FOUND).build());
      } else {
        String event = session.nextEvent();
        if (event != null) {
          response.resume(Response.ok(event).build());
        } else {
          session.registerResponse(response);
        }
      }
    }
  }

  /**
   * Event session.
   */
  static class EventSession {
    private final Queue<String> events = new ConcurrentLinkedQueue<>();
    private final Queue<AsyncResponse> responses = new ConcurrentLinkedQueue<>();

    void addEvent(String event) {
      AsyncResponse response = responses.poll();
      if (response != null) {
        response.resume(Response.ok(event).build());
      } else {
        events.add(event);
        if (events.size() > 100) {
          events.remove();
        }
      }
    }

    void registerResponse(AsyncResponse response) {
      responses.add(response);
    }

    String nextEvent() {
      return events.poll();
    }
  }

  /**
   * Event subject.
   */
  static class EventSubject extends EventSession implements Consumer<String> {
    private final Map<Integer, EventSession> sessions = new ConcurrentHashMap<>();
    private final AtomicInteger sessionId = new AtomicInteger();

    EventSession getSession(int sessionId) {
      return sessions.get(sessionId);
    }

    int newSession() {
      int sessionId = this.sessionId.incrementAndGet();
      EventSession session = new EventSession();
      sessions.put(sessionId, session);
      return sessionId;
    }

    @Override
    public void accept(String event) {
      addEvent(event);
      sessions.values().forEach(s -> s.addEvent(event));
    }
  }
}
