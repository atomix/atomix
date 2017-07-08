/*
 * Copyright 2017-present Open Networking Laboratory
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
package io.atomix.protocols.raft.proxy.impl;

import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import io.atomix.protocols.raft.RaftClient;
import io.atomix.protocols.raft.RaftException;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.protocol.CloseSessionRequest;
import io.atomix.protocols.raft.protocol.KeepAliveRequest;
import io.atomix.protocols.raft.protocol.OpenSessionRequest;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.proxy.RaftProxyClient;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadPoolContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client session manager.
 */
public class RaftProxyManager {
  private final Logger log;
  private final String clientId;
  private final MemberId memberId;
  private final RaftClientProtocol protocol;
  private final RaftProxyConnection connection;
  private final ScheduledExecutorService threadPoolExecutor;
  private final MemberSelectorManager selectorManager;
  private final Map<Long, RaftProxyState> sessions = new ConcurrentHashMap<>();
  private final AtomicBoolean open = new AtomicBoolean();
  private ScheduledFuture<?> keepAliveFuture;

  public RaftProxyManager(String clientId, MemberId memberId, RaftClientProtocol protocol, MemberSelectorManager selectorManager, ScheduledExecutorService threadPoolExecutor) {
    this.clientId = checkNotNull(clientId, "clientId cannot be null");
    this.memberId = checkNotNull(memberId, "nodeId cannot be null");
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.selectorManager = checkNotNull(selectorManager, "selectorManager cannot be null");
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(RaftClient.class)
        .addValue(clientId)
        .build());

    this.connection = new RaftProxyConnection(
        protocol,
        selectorManager.createSelector(CommunicationStrategy.ANY),
        new ThreadPoolContext(threadPoolExecutor),
        LoggerContext.builder(RaftClient.class)
            .addValue(clientId)
            .build());
    this.threadPoolExecutor = checkNotNull(threadPoolExecutor, "threadPoolExecutor cannot be null");
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
   * @param serviceType           The session type.
   * @param communicationStrategy The strategy with which to communicate with servers.
   * @param timeout               The session timeout.
   * @return A completable future to be completed once the session has been opened.
   */
  public CompletableFuture<RaftProxyClient> openSession(
      String serviceName,
      ServiceType serviceType,
      ReadConsistency readConsistency,
      CommunicationStrategy communicationStrategy,
      Duration timeout) {
    checkNotNull(serviceName, "naserviceNameme cannot be null");
    checkNotNull(serviceType, "serviceType cannot be null");
    checkNotNull(communicationStrategy, "communicationStrategy cannot be null");
    checkNotNull(timeout, "timeout cannot be null");

    log.info("Opening session; name: {}, type: {}", serviceName, serviceType);
    OpenSessionRequest request = OpenSessionRequest.newBuilder()
        .withMemberId(memberId)
        .withServiceName(serviceName)
        .withServiceType(serviceType)
        .withReadConsistency(readConsistency)
        .withTimeout(timeout.toMillis())
        .build();

    CompletableFuture<RaftProxyClient> future = new CompletableFuture<>();
    ThreadContext proxyContext = new ThreadPoolContext(threadPoolExecutor);
    connection.openSession(request).whenCompleteAsync((response, error) -> {
      if (error == null) {
        if (response.status() == RaftResponse.Status.OK) {
          // Create and store the proxy state.
          RaftProxyState state = new RaftProxyState(
              clientId,
              SessionId.from(response.session()),
              serviceName,
              serviceType,
              response.timeout());
          sessions.put(state.getSessionId().id(), state);

          // Ensure the proxy session info is reset and the session is kept alive.
          keepAliveSessions();

          // Create the proxy client and complete the future.
          RaftProxyClient client = new DefaultRaftProxyClient(
              state,
              protocol,
              selectorManager,
              this,
              communicationStrategy,
              proxyContext);

          future.complete(client);
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
   * @param sessionId The session identifier.
   * @return A completable future to be completed once the session is closed.
   */
  public CompletableFuture<Void> closeSession(SessionId sessionId) {
    RaftProxyState state = sessions.get(sessionId.id());
    if (state == null) {
      return Futures.exceptionalFuture(new RaftException.UnknownSession("Unknown session: " + sessionId));
    }

    log.info("Closing session {}", sessionId);
    CloseSessionRequest request = CloseSessionRequest.newBuilder()
        .withSession(sessionId.id())
        .build();

    CompletableFuture<Void> future = new CompletableFuture<>();
    connection.closeSession(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == RaftResponse.Status.OK) {
          sessions.remove(sessionId.id());
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
   * Resets indexes for the given session.
   *
   * @param sessionId The session for which to reset indexes.
   * @return A completable future to be completed once the session's indexes have been reset.
   */
  CompletableFuture<Void> resetIndexes(SessionId sessionId) {
    RaftProxyState sessionState = sessions.get(sessionId.id());
    if (sessionState == null) {
      return Futures.exceptionalFuture(new IllegalArgumentException("Unknown session: " + sessionId));
    }

    CompletableFuture<Void> future = new CompletableFuture<>();

    KeepAliveRequest request = KeepAliveRequest.newBuilder()
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
  private void keepAliveSessions() {
    keepAliveSessions(true);
  }

  /**
   * Sends a keep-alive request to the cluster.
   */
  private synchronized void keepAliveSessions(boolean retryOnFailure) {
    final long currentTime = System.currentTimeMillis();

    // Filter the list of sessions that need keep-alive requests to be sent.
    // If a session has been recently updated (a command has recently been committed via the session)
    // then sending a keep-alive for the session is redundant.
    List<RaftProxyState> needKeepAlive = sessions.values()
        .stream()
        .filter(s -> currentTime - s.getLastUpdated() > s.getSessionTimeout() / 2)
        .collect(Collectors.toList());

    // If no sessions need keep-alives to be sent, skip and reschedule the keep-alive.
    if (needKeepAlive.isEmpty()) {
      scheduleKeepAlive();
      return;
    }

    // Allocate session IDs, command response sequence numbers, and event index arrays.
    long[] sessionIds = new long[needKeepAlive.size()];
    long[] commandResponses = new long[needKeepAlive.size()];
    long[] eventIndexes = new long[needKeepAlive.size()];

    // For each session that needs to be kept alive, populate batch request arrays.
    int i = 0;
    for (RaftProxyState sessionState : needKeepAlive) {
      if (currentTime - sessionState.getLastUpdated() > sessionState.getSessionTimeout() / 4) {
        sessionIds[i] = sessionState.getSessionId().id();
        commandResponses[i] = sessionState.getCommandResponse();
        eventIndexes[i] = sessionState.getEventIndex();
        i++;
      }
    }

    log.debug("Keeping {} sessions alive", sessionIds.length);

    KeepAliveRequest request = KeepAliveRequest.newBuilder()
        .withSessionIds(sessionIds)
        .withCommandSequences(commandResponses)
        .withEventIndexes(eventIndexes)
        .build();

    connection.keepAlive(request).whenComplete((response, error) -> {
      if (open.get()) {
        if (error == null) {
          // If the request was successful, update the address selector and schedule the next keep-alive.
          if (response.status() == RaftResponse.Status.OK) {
            selectorManager.resetAll(response.leader(), response.members());

            // Iterate through sessions and close sessions that weren't kept alive by the request (have already been closed).
            Set<Long> keptAliveSessions = Sets.newHashSet(Longs.asList(response.sessionIds()));
            for (RaftProxyState session : needKeepAlive) {
              if (keptAliveSessions.contains(session.getSessionId().id())) {
                session.setState(RaftProxyClient.State.CONNECTED);
              } else {
                session.setState(RaftProxyClient.State.CLOSED);
              }
            }
            scheduleKeepAlive();
          }
          // If a leader is still set in the address selector, unset the leader and attempt to send another keep-alive.
          // This will ensure that the address selector selects all servers without filtering on the leader.
          else if (retryOnFailure && connection.leader() != null) {
            selectorManager.resetAll(null, connection.servers());
            keepAliveSessions(false);
          }
          // If no leader was set, set the session state to unstable and schedule another keep-alive.
          else {
            needKeepAlive.forEach(s -> s.setState(RaftProxy.State.SUSPENDED));
            selectorManager.resetAll();
            scheduleKeepAlive();
          }
        }
        // If a leader is still set in the address selector, unset the leader and attempt to send another keep-alive.
        // This will ensure that the address selector selects all servers without filtering on the leader.
        else if (retryOnFailure && connection.leader() != null) {
          selectorManager.resetAll(null, connection.servers());
          keepAliveSessions(false);
        }
        // If no leader was set, set the session state to unstable and schedule another keep-alive.
        else {
          needKeepAlive.forEach(s -> s.setState(RaftProxy.State.SUSPENDED));
          selectorManager.resetAll();
          scheduleKeepAlive();
        }
      }
    });
  }

  /**
   * Schedules a keep-alive request.
   */
  private void scheduleKeepAlive() {
    OptionalLong minTimeout = sessions.values().stream().mapToLong(RaftProxyState::getSessionTimeout).min();
    if (minTimeout.isPresent()) {
      synchronized (this) {
        if (keepAliveFuture != null) {
          keepAliveFuture.cancel(false);
        }

        keepAliveFuture = threadPoolExecutor.schedule(() -> {
          if (open.get()) {
            keepAliveSessions();
          }
        }, minTimeout.getAsLong() / 2, TimeUnit.MILLISECONDS);
      }
    }
  }

  /**
   * Closes the session manager.
   *
   * @return A completable future to be completed once the session manager is closed.
   */
  public CompletableFuture<Void> close() {
    if (open.compareAndSet(true, false)) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      threadPoolExecutor.execute(() -> {
        synchronized (this) {
          if (keepAliveFuture != null) {
            keepAliveFuture.cancel(false);
            keepAliveFuture = null;
          }
        }
        future.complete(null);
      });
      return future;
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Kills the client session manager.
   *
   * @return A completable future to be completed once the session manager is killed.
   */
  public CompletableFuture<Void> kill() {
    return CompletableFuture.runAsync(() -> {
      synchronized (this) {
        if (keepAliveFuture != null) {
          keepAliveFuture.cancel(false);
        }
      }
    }, threadPoolExecutor);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("client", clientId)
        .toString();
  }

}
