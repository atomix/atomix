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

import io.atomix.logging.Logger;
import io.atomix.logging.LoggerFactory;
import io.atomix.protocols.raft.CommunicationStrategies;
import io.atomix.protocols.raft.CommunicationStrategy;
import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.error.UnknownSessionException;
import io.atomix.protocols.raft.protocol.CloseSessionRequest;
import io.atomix.protocols.raft.protocol.KeepAliveRequest;
import io.atomix.protocols.raft.protocol.OpenSessionRequest;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.protocol.RaftResponse;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.serializer.Serializer;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.SingleThreadContext;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.concurrent.ThreadPoolContext;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Client session manager.
 */
public class RaftProxyManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(RaftProxyManager.class);
  private final String clientId;
  private final MemberId nodeId;
  private final RaftClientProtocol protocol;
  private final RaftProxyConnection connection;
  private final ScheduledExecutorService threadPoolExecutor;
  private final NodeSelectorManager selectorManager;
  private final Map<Long, RaftProxyState> sessions = new ConcurrentHashMap<>();
  private final AtomicBoolean open = new AtomicBoolean();
  private ScheduledFuture<?> keepAliveFuture;

  public RaftProxyManager(String clientId, MemberId nodeId, RaftClientProtocol protocol, NodeSelectorManager selectorManager, ScheduledExecutorService threadPoolExecutor) {
    this.clientId = checkNotNull(clientId, "clientId cannot be null");
    this.nodeId = checkNotNull(nodeId, "nodeId cannot be null");
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.selectorManager = checkNotNull(selectorManager, "selectorManager cannot be null");
    this.connection = new RaftProxyConnection(
        clientId,
        protocol,
        selectorManager.createSelector(CommunicationStrategies.ANY),
        new ThreadPoolContext(threadPoolExecutor));
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
   * @param name                  The session name.
   * @param stateMachine          The session type.
   * @param communicationStrategy The strategy with which to communicate with servers.
   * @param timeout               The session timeout.
   * @return A completable future to be completed once the session has been opened.
   */
  public CompletableFuture<RaftProxy> openSession(
      String name,
      String stateMachine,
      CommunicationStrategy communicationStrategy,
      Serializer serializer,
      Duration timeout) {
    checkNotNull(name, "name cannot be null");
    checkNotNull(stateMachine, "stateMachine cannot be null");
    checkNotNull(communicationStrategy, "communicationStrategy cannot be null");
    checkNotNull(serializer, "serializer cannot be null");
    checkNotNull(timeout, "timeout cannot be null");

    LOGGER.trace("{} - Opening session; name: {}, type: {}", clientId, name, stateMachine);
    OpenSessionRequest request = OpenSessionRequest.builder()
        .withMember(nodeId)
        .withStateMachine(stateMachine)
        .withName(name)
        .withTimeout(timeout.toMillis())
        .build();

    LOGGER.trace("{} - Sending {}", clientId, request);
    CompletableFuture<RaftProxy> future = new CompletableFuture<>();
    ThreadContext proxyContext = new SingleThreadContext(threadPoolExecutor);
    connection.openSession(request).whenCompleteAsync((response, error) -> {
      if (error == null) {
        if (response.status() == RaftResponse.Status.OK) {
          RaftProxyState state = new RaftProxyState(
              response.session(), name, stateMachine, response.timeout());
          sessions.put(state.getSessionId(), state);
          keepAliveSessions();
          future.complete(new DefaultRaftProxy(
              state,
              protocol,
              selectorManager,
              this,
              communicationStrategy,
              serializer,
              proxyContext));
        } else {
          future.completeExceptionally(response.error().createException());
        }
      } else {
        future.completeExceptionally(error);
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
  public CompletableFuture<Void> closeSession(long sessionId) {
    RaftProxyState state = sessions.get(sessionId);
    if (state == null) {
      return Futures.exceptionalFuture(new UnknownSessionException("Unknown session: " + sessionId));
    }

    LOGGER.trace("Closing session {}", sessionId);
    CloseSessionRequest request = CloseSessionRequest.builder()
        .withSession(sessionId)
        .build();

    LOGGER.trace("Sending {}", request);
    CompletableFuture<Void> future = new CompletableFuture<>();
    connection.closeSession(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == RaftResponse.Status.OK) {
          sessions.remove(sessionId);
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
  CompletableFuture<Void> resetIndexes(long sessionId) {
    RaftProxyState sessionState = sessions.get(sessionId);
    if (sessionState == null) {
      return Futures.exceptionalFuture(new IllegalArgumentException("Unknown session: " + sessionId));
    }

    CompletableFuture<Void> future = new CompletableFuture<>();

    KeepAliveRequest request = KeepAliveRequest.builder()
        .withSessionIds(new long[]{sessionId})
        .withCommandSequences(new long[]{sessionState.getCommandResponse()})
        .withEventIndexes(new long[]{sessionState.getEventIndex()})
        .build();

    LOGGER.trace("{} - Sending {}", clientId, request);
    connection.keepAlive(request).whenComplete((response, error) -> {
      if (error == null) {
        LOGGER.trace("{} - Received {}", clientId, response);
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
    Map<Long, RaftProxyState> sessions = new HashMap<>(this.sessions);
    long[] sessionIds = new long[sessions.size()];
    long[] commandResponses = new long[sessions.size()];
    long[] eventIndexes = new long[sessions.size()];
    long[] connections = new long[sessions.size()];

    int i = 0;
    for (RaftProxyState sessionState : sessions.values()) {
      sessionIds[i] = sessionState.getSessionId();
      commandResponses[i] = sessionState.getCommandResponse();
      eventIndexes[i] = sessionState.getEventIndex();
      i++;
    }

    KeepAliveRequest request = KeepAliveRequest.builder()
        .withSessionIds(sessionIds)
        .withCommandSequences(commandResponses)
        .withEventIndexes(eventIndexes)
        .build();

    LOGGER.trace("{} - Sending {}", clientId, request);
    connection.keepAlive(request).whenComplete((response, error) -> {
      if (open.get()) {
        if (error == null) {
          LOGGER.trace("{} - Received {}", clientId, response);
          // If the request was successful, update the address selector and schedule the next keep-alive.
          if (response.status() == RaftResponse.Status.OK) {
            selectorManager.resetAll(response.leader(), response.members());
            sessions.values().forEach(s -> s.setState(RaftProxy.State.CONNECTED));
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
            sessions.values().forEach(s -> s.setState(RaftProxy.State.SUSPENDED));
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
          sessions.values().forEach(s -> s.setState(RaftProxy.State.SUSPENDED));
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
