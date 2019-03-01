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
package io.atomix.protocols.raft.session.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.service.ServiceConfig;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.RaftClient;
import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.protocol.CloseSessionRequest;
import io.atomix.protocols.raft.protocol.HeartbeatRequest;
import io.atomix.protocols.raft.protocol.HeartbeatResponse;
import io.atomix.protocols.raft.protocol.KeepAliveRequest;
import io.atomix.protocols.raft.protocol.OpenSessionRequest;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.session.CommunicationStrategy;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.Scheduled;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadContextFactory;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import io.atomix.utils.serializer.Serializer;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client session manager.
 */
public class RaftSessionManager {
  private static final double TIMEOUT_FACTOR = .5;
  private static final long MIN_TIMEOUT_DELTA = 2500;

  private final Logger log;
  private final String clientId;
  private final MemberId memberId;
  private final RaftClientProtocol protocol;
  private final RaftSessionConnection connection;
  private final ThreadContextFactory threadContextFactory;
  private final ThreadContext threadContext;
  private final MemberSelectorManager selectorManager;
  private final Map<Long, RaftSessionState> sessions = new ConcurrentHashMap<>();
  private final Map<Long, Scheduled> keepAliveTimers = new ConcurrentHashMap<>();
  private final AtomicBoolean open = new AtomicBoolean();

  public RaftSessionManager(String clientId, MemberId memberId, RaftClientProtocol protocol, MemberSelectorManager selectorManager, ThreadContextFactory threadContextFactory) {
    this.clientId = checkNotNull(clientId, "clientId cannot be null");
    this.memberId = checkNotNull(memberId, "memberId cannot be null");
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.selectorManager = checkNotNull(selectorManager, "selectorManager cannot be null");
    this.threadContext = threadContextFactory.createContext();
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(RaftClient.class)
        .addValue(clientId)
        .build());

    this.connection = new RaftSessionConnection(
        protocol,
        selectorManager.createSelector(CommunicationStrategy.LEADER),
        threadContextFactory.createContext(),
        LoggerContext.builder(RaftClient.class)
            .addValue(clientId)
            .build());
    protocol.registerHeartbeatHandler(this::handleHeartbeat);
    this.threadContextFactory = checkNotNull(threadContextFactory, "threadContextFactory cannot be null");
  }

  /**
   * Returns the current term.
   *
   * @return the current term
   */
  public long term() {
    return 0; // TODO
  }

  /**
   * Returns the current leader.
   *
   * @return the current leader
   */
  public MemberId leader() {
    return selectorManager.leader();
  }

  /**
   * Resets the session manager's cluster information.
   */
  public void resetConnections() {
    selectorManager.resetAll();
  }

  /**
   * Resets the session manager's cluster information.
   *
   * @param leader  The leader address.
   * @param servers The collection of servers.
   */
  public void resetConnections(MemberId leader, Collection<MemberId> servers) {
    selectorManager.resetAll(leader, servers);
  }

  /**
   * Opens the session manager.
   *
   * @return A completable future to be called once the session manager is opened.
   */
  public CompletableFuture<Void> open() {
    open.set(true);
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Opens a new session.
   *
   * @param serviceName           The session name.
   * @param primitiveType         The session type.
   * @param communicationStrategy The strategy with which to communicate with servers.
   * @param minTimeout            The minimum session timeout.
   * @param maxTimeout            The maximum session timeout.
   * @return A completable future to be completed once the session has been opened.
   */
  public CompletableFuture<RaftSessionState> openSession(
      String serviceName,
      PrimitiveType primitiveType,
      ServiceConfig config,
      ReadConsistency readConsistency,
      CommunicationStrategy communicationStrategy,
      Duration minTimeout,
      Duration maxTimeout) {
    checkNotNull(serviceName, "serviceName cannot be null");
    checkNotNull(primitiveType, "serviceType cannot be null");
    checkNotNull(communicationStrategy, "communicationStrategy cannot be null");
    checkNotNull(maxTimeout, "timeout cannot be null");

    log.debug("Opening session; name: {}, type: {}", serviceName, primitiveType);
    OpenSessionRequest request = OpenSessionRequest.builder()
        .withMemberId(memberId)
        .withServiceName(serviceName)
        .withServiceType(primitiveType)
        .withServiceConfig(Serializer.using(primitiveType.namespace()).encode(config))
        .withReadConsistency(readConsistency)
        .withMinTimeout(minTimeout.toMillis())
        .withMaxTimeout(maxTimeout.toMillis())
        .build();

    CompletableFuture<RaftSessionState> future = new CompletableFuture<>();
    ThreadContext proxyContext = threadContextFactory.createContext();
    connection.openSession(request).whenCompleteAsync((response, error) -> {
      if (error == null) {
        if (response.status() == RaftResponse.Status.OK) {
          // Create and store the proxy state.
          RaftSessionState state = new RaftSessionState(
              clientId,
              SessionId.from(response.session()),
              serviceName,
              primitiveType,
              response.timeout());
          sessions.put(state.getSessionId().id(), state);

          state.addStateChangeListener(s -> {
            if (s == PrimitiveState.EXPIRED || s == PrimitiveState.CLOSED) {
              sessions.remove(state.getSessionId().id());
            }
          });

          // Ensure the proxy session info is reset and the session is kept alive.
          keepAliveSessions(System.currentTimeMillis(), state.getSessionTimeout());

          future.complete(state);
        } else {
          future.completeExceptionally(new RaftException.Unavailable(response.error().message()));
        }
      } else {
        future.completeExceptionally(new RaftException.Unavailable(error.getMessage()));
      }
    }, proxyContext);
    return future;
  }

  /**
   * Closes a session.
   *
   * @param sessionId the session identifier.
   * @param delete whether to delete the service
   * @return A completable future to be completed once the session is closed.
   */
  public CompletableFuture<Void> closeSession(SessionId sessionId, boolean delete) {
    RaftSessionState state = sessions.get(sessionId.id());
    if (state == null) {
      return Futures.exceptionalFuture(new RaftException.UnknownSession("Unknown session: " + sessionId));
    }

    log.debug("Closing session {}", sessionId);
    CloseSessionRequest request = CloseSessionRequest.builder()
        .withSession(sessionId.id())
        .withDelete(delete)
        .build();

    CompletableFuture<Void> future = new CompletableFuture<>();
    connection.closeSession(request).whenComplete((response, error) -> {
      sessions.remove(sessionId.id());
      if (error == null) {
        if (response.status() == RaftResponse.Status.OK) {
          future.complete(null);
        } else {
          future.completeExceptionally(response.error().createException());
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Resets indexes for all sessions.
   */
  private synchronized void resetAllIndexes() {
    Collection<RaftSessionState> sessions = Lists.newArrayList(this.sessions.values());

    // If no sessions are open, skip the keep-alive.
    if (sessions.isEmpty()) {
      return;
    }

    // Allocate session IDs, command response sequence numbers, and event index arrays.
    long[] sessionIds = new long[sessions.size()];
    long[] commandResponses = new long[sessions.size()];
    long[] eventIndexes = new long[sessions.size()];

    // For each session that needs to be kept alive, populate batch request arrays.
    int i = 0;
    for (RaftSessionState sessionState : sessions) {
      sessionIds[i] = sessionState.getSessionId().id();
      commandResponses[i] = sessionState.getCommandResponse();
      eventIndexes[i] = sessionState.getEventIndex();
      i++;
    }

    log.trace("Resetting {} sessions", sessionIds.length);

    KeepAliveRequest request = KeepAliveRequest.builder()
        .withSessionIds(sessionIds)
        .withCommandSequences(commandResponses)
        .withEventIndexes(eventIndexes)
        .build();
    connection.keepAlive(request);
  }

  /**
   * Resets indexes for the given session.
   *
   * @param sessionId The session for which to reset indexes.
   * @return A completable future to be completed once the session's indexes have been reset.
   */
  CompletableFuture<Void> resetIndexes(SessionId sessionId) {
    RaftSessionState sessionState = sessions.get(sessionId.id());
    if (sessionState == null) {
      return Futures.exceptionalFuture(new IllegalArgumentException("Unknown session: " + sessionId));
    }

    CompletableFuture<Void> future = new CompletableFuture<>();

    KeepAliveRequest request = KeepAliveRequest.builder()
        .withSessionIds(new long[]{sessionId.id()})
        .withCommandSequences(new long[]{sessionState.getCommandResponse()})
        .withEventIndexes(new long[]{sessionState.getEventIndex()})
        .build();

    connection.keepAlive(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == RaftResponse.Status.OK) {
          future.complete(null);
        } else {
          future.completeExceptionally(response.error().createException());
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  /**
   * Sends a keep-alive request to the cluster.
   */
  private synchronized void keepAliveSessions(long lastKeepAliveTime, long sessionTimeout) {
    // Filter the list of sessions by timeout.
    List<RaftSessionState> needKeepAlive = sessions.values()
        .stream()
        .filter(session -> session.getSessionTimeout() == sessionTimeout)
        .collect(Collectors.toList());

    // If no sessions need keep-alives to be sent, skip and reschedule the keep-alive.
    if (needKeepAlive.isEmpty()) {
      return;
    }

    // Allocate session IDs, command response sequence numbers, and event index arrays.
    long[] sessionIds = new long[needKeepAlive.size()];
    long[] commandResponses = new long[needKeepAlive.size()];
    long[] eventIndexes = new long[needKeepAlive.size()];

    // For each session that needs to be kept alive, populate batch request arrays.
    int i = 0;
    for (RaftSessionState sessionState : needKeepAlive) {
      sessionIds[i] = sessionState.getSessionId().id();
      commandResponses[i] = sessionState.getCommandResponse();
      eventIndexes[i] = sessionState.getEventIndex();
      i++;
    }

    log.trace("Keeping {} sessions alive", sessionIds.length);

    KeepAliveRequest request = KeepAliveRequest.builder()
        .withSessionIds(sessionIds)
        .withCommandSequences(commandResponses)
        .withEventIndexes(eventIndexes)
        .build();

    long keepAliveTime = System.currentTimeMillis();
    connection.keepAlive(request).whenComplete((response, error) -> {
      if (open.get()) {
        long delta = System.currentTimeMillis() - keepAliveTime;
        if (error == null) {
          // If the request was successful, update the address selector and schedule the next keep-alive.
          if (response.status() == RaftResponse.Status.OK) {
            selectorManager.resetAll(response.leader(), response.members());

            // Iterate through sessions and close sessions that weren't kept alive by the request (have already been closed).
            Set<Long> keptAliveSessions = Sets.newHashSet(Longs.asList(response.sessionIds()));
            for (RaftSessionState session : needKeepAlive) {
              if (keptAliveSessions.contains(session.getSessionId().id())) {
                session.setState(PrimitiveState.CONNECTED);
              } else {
                session.setState(PrimitiveState.EXPIRED);
              }
            }
            scheduleKeepAlive(System.currentTimeMillis(), sessionTimeout, delta);
          }
          // If the timeout has not been passed, attempt to keep the session alive again with no delay.
          // We will continue to retry until the session expiration has passed.
          else if (System.currentTimeMillis() - lastKeepAliveTime < sessionTimeout) {
            selectorManager.resetAll(null, connection.members());
            keepAliveSessions(lastKeepAliveTime, sessionTimeout);
          }
          // If no leader was set, set the session state to unstable and schedule another keep-alive.
          else {
            needKeepAlive.forEach(s -> s.setState(PrimitiveState.SUSPENDED));
            selectorManager.resetAll();
            scheduleKeepAlive(lastKeepAliveTime, sessionTimeout, delta);
          }
        }
        // If the timeout has not been passed, reset the connection and attempt to keep the session alive
        // again with no delay.
        else if (System.currentTimeMillis() - lastKeepAliveTime < sessionTimeout && connection.leader() != null) {
          selectorManager.resetAll(null, connection.members());
          keepAliveSessions(lastKeepAliveTime, sessionTimeout);
        }
        // If no leader was set, set the session state to unstable and schedule another keep-alive.
        else {
          needKeepAlive.forEach(s -> s.setState(PrimitiveState.SUSPENDED));
          selectorManager.resetAll();
          scheduleKeepAlive(lastKeepAliveTime, sessionTimeout, delta);
        }
      }
    });
  }

  /**
   * Schedules a keep-alive request.
   */
  private synchronized void scheduleKeepAlive(long lastKeepAliveTime, long timeout, long delta) {
    Scheduled keepAliveFuture = keepAliveTimers.remove(timeout);
    if (keepAliveFuture != null) {
      keepAliveFuture.cancel();
    }

    // Schedule the keep alive for 3/4 the timeout minus the delta from the last keep-alive request.
    keepAliveTimers.put(timeout, threadContext.schedule(Duration.ofMillis(Math.max(Math.max((long) (timeout * TIMEOUT_FACTOR) - delta, timeout - MIN_TIMEOUT_DELTA - delta), 0)), () -> {
      if (open.get()) {
        keepAliveSessions(lastKeepAliveTime, timeout);
      }
    }));
  }

  /**
   * Handles a heartbeat request.
   */
  private CompletableFuture<HeartbeatResponse> handleHeartbeat(HeartbeatRequest request) {
    log.trace("Received {}", request);
    boolean newLeader = !Objects.equals(selectorManager.leader(), request.leader());
    selectorManager.resetAll(request.leader(), request.members());
    HeartbeatResponse response = HeartbeatResponse.builder()
        .withStatus(RaftResponse.Status.OK)
        .build();
    if (newLeader) {
      resetAllIndexes();
    }
    log.trace("Sending {}", response);
    return CompletableFuture.completedFuture(response);
  }

  /**
   * Closes the session manager.
   *
   * @return A completable future to be completed once the session manager is closed.
   */
  public CompletableFuture<Void> close() {
    if (open.compareAndSet(true, false)) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      threadContext.execute(() -> {
        synchronized (this) {
          for (Scheduled keepAliveFuture : keepAliveTimers.values()) {
            keepAliveFuture.cancel();
          }
          protocol.unregisterHeartbeatHandler();
        }
        future.complete(null);
      });
      return future;
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("client", clientId)
        .toString();
  }

}
