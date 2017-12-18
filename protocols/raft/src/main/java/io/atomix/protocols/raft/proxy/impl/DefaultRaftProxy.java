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

import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.proxy.impl.AbstractPrimitiveProxy;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.raft.ReadConsistency;
import io.atomix.protocols.raft.protocol.RaftClientProtocol;
import io.atomix.protocols.raft.proxy.CommunicationStrategy;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.LoggerContext;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Handles submitting state machine {@link PrimitiveOperation operations} to the Raft cluster.
 * <p>
 * The client session is responsible for maintaining a client's connection to a Raft cluster and coordinating
 * the submission of {@link PrimitiveOperation operations} to various nodes in the cluster. Client
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
public class DefaultRaftProxy extends AbstractPrimitiveProxy implements RaftProxy {
  private final String serviceName;
  private final PrimitiveType primitiveType;
  private final Duration minTimeout;
  private final Duration maxTimeout;
  private final RaftClientProtocol protocol;
  private final MemberSelectorManager selectorManager;
  private final RaftProxyManager sessionManager;
  private final ReadConsistency readConsistency;
  private final CommunicationStrategy communicationStrategy;
  private final ThreadContext context;
  private volatile RaftProxyListener proxyListener;
  private volatile RaftProxyInvoker proxyInvoker;
  private volatile RaftProxyState state;

  public DefaultRaftProxy(
      String serviceName,
      PrimitiveType primitiveType,
      RaftClientProtocol protocol,
      MemberSelectorManager selectorManager,
      RaftProxyManager sessionManager,
      ReadConsistency readConsistency,
      CommunicationStrategy communicationStrategy,
      ThreadContext context,
      Duration minTimeout,
      Duration maxTimeout) {
    this.serviceName = checkNotNull(serviceName, "serviceName cannot be null");
    this.primitiveType = checkNotNull(primitiveType, "serviceType cannot be null");
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.selectorManager = checkNotNull(selectorManager, "selectorManager cannot be null");
    this.readConsistency = checkNotNull(readConsistency, "readConsistency cannot be null");
    this.communicationStrategy = checkNotNull(communicationStrategy, "communicationStrategy cannot be null");
    this.context = checkNotNull(context, "context cannot be null");
    this.minTimeout = checkNotNull(minTimeout, "minTimeout cannot be null");
    this.maxTimeout = checkNotNull(maxTimeout, "maxTimeout cannot be null");
    this.sessionManager = checkNotNull(sessionManager, "sessionManager cannot be null");
  }

  @Override
  public String name() {
    return serviceName;
  }

  @Override
  public PrimitiveType serviceType() {
    return primitiveType;
  }

  @Override
  public SessionId sessionId() {
    return state != null ? state.getSessionId() : null;
  }

  @Override
  public PrimitiveProxy.State getState() {
    return state.getState();
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveProxy.State> listener) {
    if (state != null) {
      state.addStateChangeListener(listener);
    }
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveProxy.State> listener) {
    if (state != null) {
      state.removeStateChangeListener(listener);
    }
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    RaftProxyInvoker invoker = this.proxyInvoker;
    if (invoker == null) {
      return Futures.exceptionalFuture(new IllegalStateException("Session not open"));
    }
    return invoker.invoke(operation);
  }

  @Override
  public void addEventListener(Consumer<PrimitiveEvent> listener) {
    if (proxyListener != null) {
      proxyListener.addEventListener(listener);
    }
  }

  @Override
  public void removeEventListener(Consumer<PrimitiveEvent> listener) {
    if (proxyListener != null) {
      proxyListener.removeEventListener(listener);
    }
  }

  @Override
  public CompletableFuture<PrimitiveProxy> connect() {
    return sessionManager.openSession(
        serviceName,
        primitiveType,
        readConsistency,
        communicationStrategy,
        minTimeout,
        maxTimeout)
        .thenApply(state -> {
          this.state = state;

          // Create command/query connections.
          RaftProxyConnection leaderConnection = new RaftProxyConnection(
              protocol,
              selectorManager.createSelector(CommunicationStrategy.LEADER),
              context,
              LoggerContext.builder(PrimitiveProxy.class)
                  .addValue(state.getSessionId())
                  .add("type", state.getPrimitiveType())
                  .add("name", state.getPrimitiveName())
                  .build());
          RaftProxyConnection sessionConnection = new RaftProxyConnection(
              protocol,
              selectorManager.createSelector(communicationStrategy),
              context,
              LoggerContext.builder(PrimitiveProxy.class)
                  .addValue(state.getSessionId())
                  .add("type", state.getPrimitiveType())
                  .add("name", state.getPrimitiveName())
                  .build());

          // Create proxy submitter/listener.
          RaftProxySequencer sequencer = new RaftProxySequencer(state);
          this.proxyListener = new RaftProxyListener(
              protocol,
              selectorManager.createSelector(CommunicationStrategy.ANY),
              state,
              sequencer,
              context);
          this.proxyInvoker = new RaftProxyInvoker(
              leaderConnection,
              sessionConnection,
              state,
              sequencer,
              sessionManager,
              context);

          return this;
        });
  }

  @Override
  public CompletableFuture<Void> close() {
    if (state != null) {
      return sessionManager.closeSession(state.getSessionId())
          .whenComplete((result, error) -> state.setState(PrimitiveProxy.State.CLOSED));
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public int hashCode() {
    return Objects.hash(state);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof DefaultRaftProxy
        && ((DefaultRaftProxy) object).state.getSessionId() == state.getSessionId();
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("session", state != null ? state.getSessionId() : null)
        .toString();
  }
}
