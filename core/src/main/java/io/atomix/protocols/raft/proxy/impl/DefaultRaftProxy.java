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

import io.atomix.protocols.raft.RaftCommand;
import io.atomix.protocols.raft.RaftOperation;
import io.atomix.protocols.raft.RaftQuery;
import io.atomix.protocols.raft.CommunicationStrategies;
import io.atomix.protocols.raft.CommunicationStrategy;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.util.serializer.Serializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Handles submitting state machine {@link RaftCommand commands} and {@link RaftQuery queries} to the Copycat cluster.
 * <p>
 * The client session is responsible for maintaining a client's connection to a Copycat cluster and coordinating
 * the submission of {@link RaftCommand commands} and {@link RaftQuery queries} to various nodes in the cluster. Client
 * sessions are single-use objects that represent the context within which a cluster can guarantee linearizable
 * semantics for state machine operations. When a session is opened, the session will register
 * itself with the cluster by attempting to contact each of the known servers. Once the session has been successfully
 * registered, kee-alive requests will be periodically sent to keep the session alive.
 * <p>
 * Sessions are responsible for sequencing concurrent operations to ensure they're applied to the system state
 * in the order in which they were submitted by the client. To do so, the session coordinates with its server-side
 * counterpart using unique per-operation sequence numbers.
 * <p>
 * In the event that the client session expires, clients are responsible for opening a new session by creating and
 * opening a new session object.
 */
public class DefaultRaftProxy implements RaftProxy {
  private final RaftProxyState state;
  private final RaftProxyManager sessionManager;
  private final RaftProxyListener sessionListener;
  private final RaftProxySubmitter sessionSubmitter;

  public DefaultRaftProxy(
      RaftProxyState state,
      RaftClientProtocol protocol,
      NodeSelectorManager selectorManager,
      RaftProxyManager sessionManager,
      CommunicationStrategy communicationStrategy,
      Serializer serializer,
      Executor orderedExecutor,
      ScheduledExecutorService scheduledExecutor) {
    this.state = checkNotNull(state, "state cannot be null");
    this.sessionManager = checkNotNull(sessionManager, "sessionManager cannot be null");
    RaftProxySequencer sequencer = new RaftProxySequencer(state);
    this.sessionListener = new RaftProxyListener(protocol, state, sequencer, serializer, orderedExecutor);
    RaftConnection leaderConnection = new RaftConnection(String.valueOf(state.getSessionId()), protocol.dispatcher(), selectorManager.createSelector(CommunicationStrategies.LEADER));
    RaftConnection sessionConnection = new RaftConnection(String.valueOf(state.getSessionId()), protocol.dispatcher(), selectorManager.createSelector(communicationStrategy));
    this.sessionSubmitter = new RaftProxySubmitter(leaderConnection, sessionConnection, state, sequencer, sessionManager, serializer, orderedExecutor, scheduledExecutor);
  }

  @Override
  public String name() {
    return state.getSessionName();
  }

  @Override
  public String type() {
    return state.getSessionType();
  }

  @Override
  public State state() {
    return state.getState();
  }

  @Override
  public void addStateChangeListener(Consumer<State> listener) {
    state.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<State> listener) {
    state.removeStateChangeListener(listener);
  }

  /**
   * Submits an operation to the session.
   *
   * @param operation The operation to submit.
   * @param <T>       The operation result type.
   * @return A completable future to be completed with the operation result.
   */
  public <T> CompletableFuture<T> submit(RaftOperation<T> operation) {
    if (operation instanceof RaftQuery) {
      return submit((RaftQuery<T>) operation);
    } else if (operation instanceof RaftCommand) {
      return submit((RaftCommand<T>) operation);
    } else {
      throw new UnsupportedOperationException("unknown operation type: " + operation.getClass());
    }
  }

  /**
   * Submits a command to the session.
   *
   * @param command The command to submit.
   * @param <T>     The command result type.
   * @return A completable future to be completed with the command result.
   */
  public <T> CompletableFuture<T> submit(RaftCommand<T> command) {
    return sessionSubmitter.submit(command);
  }

  /**
   * Submits a query to the session.
   *
   * @param query The query to submit.
   * @param <T>   The query result type.
   * @return A completable future to be completed with the query result.
   */
  public <T> CompletableFuture<T> submit(RaftQuery<T> query) {
    return sessionSubmitter.submit(query);
  }

  @Override
  public <T> void addEventListener(Consumer<T> callback) {
    sessionListener.addEventListener(callback);
  }

  @Override
  public <T> void removeEventListener(Consumer<T> callback) {
    sessionListener.removeEventListener(callback);
  }

  @Override
  public boolean isOpen() {
    return state.getState() != State.CLOSED;
  }

  @Override
  public CompletableFuture<Void> close() {
    return sessionManager.closeSession(state.getSessionId()).whenComplete((result, error) -> state.setState(State.CLOSED));
  }

  @Override
  public int hashCode() {
    int hashCode = 31;
    long id = state.getSessionId();
    hashCode = 37 * hashCode + (int) (id ^ (id >>> 32));
    return hashCode;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof DefaultRaftProxy && ((DefaultRaftProxy) object).state.getSessionId() == state.getSessionId();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", state.getSessionId())
        .toString();
  }

}
