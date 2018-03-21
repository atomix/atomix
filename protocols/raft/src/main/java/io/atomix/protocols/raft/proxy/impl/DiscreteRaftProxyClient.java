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
package io.atomix.protocols.raft.proxy.impl;

import io.atomix.protocols.raft.cluster.MemberId;
import io.atomix.protocols.raft.event.RaftEvent;
import io.atomix.protocols.raft.operation.RaftOperation;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.protocols.raft.proxy.RaftProxyClient;
import io.atomix.protocols.raft.service.ServiceRevision;
import io.atomix.protocols.raft.service.ServiceType;
import io.atomix.protocols.raft.session.SessionId;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.LoggerContext;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Handles submitting state machine {@link RaftOperation operations} to the Raft cluster.
 * <p>
 * The client session is responsible for maintaining a client's connection to a Raft cluster and coordinating
 * the submission of {@link RaftOperation operations} to various nodes in the cluster. Client
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
public class DiscreteRaftProxyClient implements RaftProxyClient {
  private final RaftProxyState state;
  private final RaftProxyManager sessionManager;
  private final RaftProxyListener proxyListener;
  private final RaftProxyInvoker proxySubmitter;
  private final Consumer<MemberId> leaderChangeListener = this::onLeaderChange;

  public DiscreteRaftProxyClient(
      RaftProxyState state,
      RaftClientProtocol protocol,
      MemberSelectorManager selectorManager,
      RaftProxyManager sessionManager,
      CommunicationStrategy communicationStrategy,
      ThreadContext context) {
    this.state = checkNotNull(state, "state cannot be null");
    this.sessionManager = checkNotNull(sessionManager, "sessionManager cannot be null");

    // Create command/query connections.
    RaftProxyConnection leaderConnection = new RaftProxyConnection(
        protocol,
        selectorManager.createSelector(CommunicationStrategy.LEADER),
        context,
        LoggerContext.builder(RaftProxy.class)
            .addValue(state.getSessionId())
            .add("type", state.getServiceType())
            .add("name", state.getServiceName())
            .build());
    RaftProxyConnection sessionConnection = new RaftProxyConnection(
        protocol,
        selectorManager.createSelector(communicationStrategy),
        context,
        LoggerContext.builder(RaftProxy.class)
            .addValue(state.getSessionId())
            .add("type", state.getServiceType())
            .add("name", state.getServiceName())
            .build());

    // Create proxy submitter/listener.
    RaftProxySequencer sequencer = new RaftProxySequencer(state);
    this.proxyListener = new RaftProxyListener(
        protocol,
        selectorManager.createSelector(CommunicationStrategy.ANY),
        state,
        sequencer,
        context);
    this.proxySubmitter = new RaftProxyInvoker(
        leaderConnection,
        sessionConnection,
        state,
        sequencer,
        sessionManager,
        context);

    selectorManager.addLeaderChangeListener(leaderChangeListener);
    state.addStateChangeListener(s -> {
      if (s == RaftProxy.State.CLOSED) {
        selectorManager.removeLeaderChangeListener(leaderChangeListener);
      }
    });
  }

  private void onLeaderChange(MemberId memberId) {
    if (memberId != null) {
      proxySubmitter.reset();
    }
  }

  @Override
  public String name() {
    return state.getServiceName();
  }

  @Override
  public ServiceType serviceType() {
    return state.getServiceType();
  }

  @Override
  public SessionId sessionId() {
    return state.getSessionId();
  }

  @Override
  public ServiceRevision revision() {
    return state.getRevision();
  }

  @Override
  public RaftProxy.State getState() {
    return state.getState();
  }

  @Override
  public void addStateChangeListener(Consumer<RaftProxy.State> listener) {
    state.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<RaftProxy.State> listener) {
    state.removeStateChangeListener(listener);
  }

  @Override
  public CompletableFuture<byte[]> execute(RaftOperation operation) {
    return proxySubmitter.invoke(operation);
  }

  @Override
  public void addEventListener(Consumer<RaftEvent> listener) {
    proxyListener.addEventListener(listener);
  }

  @Override
  public void removeEventListener(Consumer<RaftEvent> listener) {
    proxyListener.removeEventListener(listener);
  }

  @Override
  public CompletableFuture<RaftProxyClient> open() {
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return state.getState() != RaftProxy.State.CLOSED;
  }

  @Override
  public CompletableFuture<Void> close() {
    return sessionManager.closeSession(state.getSessionId())
        .whenComplete((result, error) -> state.setState(RaftProxy.State.CLOSED));
  }

  @Override
  public boolean isClosed() {
    return state.getState() == RaftProxy.State.CLOSED;
  }

  @Override
  public int hashCode() {
    int hashCode = 31;
    long id = state.getSessionId().id();
    hashCode = 37 * hashCode + (int) (id ^ (id >>> 32));
    return hashCode;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof DiscreteRaftProxyClient && ((DiscreteRaftProxyClient) object).state.getSessionId() == state.getSessionId();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("id", state.getSessionId())
        .toString();
  }
}
