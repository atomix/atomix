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

import io.atomix.core.election.AsyncLeaderElection;
import io.atomix.core.election.Leadership;
import io.atomix.core.election.LeadershipEvent;
import io.atomix.core.election.LeadershipEventListener;
import io.atomix.rest.utils.EventLog;
import io.atomix.rest.utils.EventManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

/**
 * Leader elector resource.
 */
public class LeaderElectorResource {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderElectorResource.class);

  private final AsyncLeaderElection<String> leaderElector;

  public LeaderElectorResource(AsyncLeaderElection<String> leaderElector) {
    this.leaderElector = leaderElector;
  }

  /**
   * Returns an event log name for the given identifier.
   */
  private String getEventLogName(String id) {
    return String.format("%s-%s", leaderElector.name(), id);
  }

  @POST
  @Produces(MediaType.APPLICATION_JSON)
  public void run(@Context EventManager events, @Suspended AsyncResponse response) {
    String id = UUID.randomUUID().toString();
    EventLog<LeadershipEventListener<String>, LeadershipEvent<String>> eventLog = events.getOrCreateEventLog(
        AsyncLeaderElection.class, getEventLogName(id), l -> e -> l.addEvent(e));

    leaderElector.addListener(eventLog.listener()).whenComplete((listenResult, listenError) -> {
      if (listenError == null) {
        leaderElector.run(id).whenComplete((runResult, runError) -> {
          if (runError == null) {
            response.resume(Response.ok(id).build());
          } else {
            LOGGER.warn("{}", runError);
            response.resume(Response.serverError().build());
          }
        });
      } else {
        LOGGER.warn("{}", listenError);
        response.resume(Response.serverError().build());
      }
    });
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public void getLeadership(@Suspended AsyncResponse response) {
    leaderElector.getLeadership().whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok(new LeadershipResponse(result)).build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @GET
  @Path("/{id}")
  public void listen(@PathParam("id") String id, @Context EventManager events, @Suspended AsyncResponse response) {
    EventLog<LeadershipEventListener<String>, LeadershipEvent<String>> eventLog = events.getEventLog(
        AsyncLeaderElection.class, getEventLogName(id));
    consumeNextEvent(eventLog, id, response);
  }

  /**
   * Recursively consumes events from the given event log until the next event for the given ID is located.
   */
  private void consumeNextEvent(EventLog<LeadershipEventListener<String>, LeadershipEvent<String>> eventLog, String id, AsyncResponse response) {
    eventLog.nextEvent().whenComplete((event, error) -> {
      if (error == null) {
        if (event.newLeadership().leader() != null && event.newLeadership().leader().id().equals(id)) {
          response.resume(Response.ok(new LeadershipResponse(event.newLeadership())).build());
        } else if (event.newLeadership().candidates().stream().noneMatch(c -> c.equals(id))) {
          leaderElector.removeListener(eventLog.listener()).whenComplete((removeResult, removeError) -> {
            response.resume(Response.status(Status.NOT_FOUND).build());
          });
        }
      } else {
        response.resume(Response.status(Status.NOT_FOUND).build());
      }
    });
  }

  @DELETE
  @Path("/{id}")
  public void withdraw(@PathParam("id") String id, @Context EventManager events, @Suspended AsyncResponse response) {
    EventLog<LeadershipEventListener<String>, LeadershipEvent<String>> eventLog = events.removeEventLog(
        AsyncLeaderElection.class, getEventLogName(id));
    if (eventLog != null && eventLog.close()) {
      leaderElector.removeListener(eventLog.listener()).whenComplete((removeResult, removeError) -> {
        leaderElector.withdraw(id).whenComplete((withdrawResult, withdrawError) -> {
          if (withdrawError == null) {
            response.resume(Response.ok().build());
          } else {
            LOGGER.warn("{}", removeError);
            response.resume(Response.serverError().build());
          }
        });
      });
    } else {
      response.resume(Response.ok().build());
    }
  }

  @POST
  @Path("/{id}/anoint")
  public void anoint(@PathParam("id") String id, @Suspended AsyncResponse response) {
    leaderElector.anoint(id).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @POST
  @Path("/{id}/promote")
  public void promote(@PathParam("id") String id, @Suspended AsyncResponse response) {
    leaderElector.promote(id).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  @POST
  @Path("/{id}/evict")
  public void evict(@PathParam("id") String id, @Suspended AsyncResponse response) {
    leaderElector.evict(id).whenComplete((result, error) -> {
      if (error == null) {
        response.resume(Response.ok().build());
      } else {
        LOGGER.warn("{}", error);
        response.resume(Response.serverError().build());
      }
    });
  }

  /**
   * Leadership response
   */
  static class LeadershipResponse {
    private final Leadership<String> leadership;

    public LeadershipResponse(Leadership<String> leadership) {
      this.leadership = leadership;
    }

    public String getLeader() {
      return leadership.leader() != null ? leadership.leader().id() : null;
    }

    public List<String> getCandidates() {
      return leadership.candidates();
    }
  }
}
