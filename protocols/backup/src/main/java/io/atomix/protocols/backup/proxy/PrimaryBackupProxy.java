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
package io.atomix.protocols.backup.proxy;

import com.google.common.collect.Sets;
import io.atomix.cluster.ClusterEvent;
import io.atomix.cluster.ClusterEventListener;
import io.atomix.cluster.ClusterService;
import io.atomix.cluster.NodeId;
import io.atomix.cluster.messaging.ClusterCommunicationService;
import io.atomix.cluster.messaging.MessageSubject;
import io.atomix.primitive.PrimitiveException.Unavailable;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.proxy.impl.AbstractPrimitiveProxy;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.backup.ReplicaInfo;
import io.atomix.protocols.backup.ReplicaInfoProvider;
import io.atomix.protocols.backup.protocol.CloseSessionRequest;
import io.atomix.protocols.backup.protocol.CloseSessionResponse;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.ExecuteResponse;
import io.atomix.protocols.backup.protocol.OpenSessionRequest;
import io.atomix.protocols.backup.protocol.OpenSessionResponse;
import io.atomix.protocols.backup.protocol.PrimaryBackupResponse.Status;
import io.atomix.protocols.backup.serializer.impl.PrimaryBackupSerializers;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.serializer.Serializer;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Primary-backup proxy.
 */
public class PrimaryBackupProxy extends AbstractPrimitiveProxy {
  private static final Serializer SERIALIZER = PrimaryBackupSerializers.PROTOCOL;
  private final String clientName;
  private final String primitiveName;
  private final PrimitiveType primitiveType;
  private final ClusterService clusterService;
  private final ClusterCommunicationService communicationService;
  private final ReplicaInfoProvider replicaProvider;
  private final ThreadContext threadContext;
  private final Set<Consumer<State>> stateChangeListeners = Sets.newIdentityHashSet();
  private final Set<Consumer<PrimitiveEvent>> eventListeners = Sets.newIdentityHashSet();
  private final Consumer<ReplicaInfo> replicaChangeListener = this::handleReplicaChange;
  private final ClusterEventListener clusterEventListener = this::handleClusterEvent;
  private final MessageSubject openSessionSubject;
  private final MessageSubject closeSessionSubject;
  private NodeId primary;
  private MessageSubject executeSubject;
  private MessageSubject eventSubject;
  private SessionId sessionId;
  private volatile State state = State.CLOSED;

  public PrimaryBackupProxy(
      String clientName,
      String primitiveName,
      PrimitiveType primitiveType,
      ClusterService clusterService,
      ClusterCommunicationService communicationService,
      ReplicaInfoProvider replicaProvider,
      ThreadContext threadContext) {
    this.clientName = clientName;
    this.primitiveName = primitiveName;
    this.primitiveType = primitiveType;
    this.clusterService = clusterService;
    this.communicationService = communicationService;
    this.replicaProvider = replicaProvider;
    this.threadContext = threadContext;
    openSessionSubject = new MessageSubject(String.format("%s-open", clientName));
    closeSessionSubject = new MessageSubject(String.format("%s-close", clientName));
  }

  @Override
  public SessionId sessionId() {
    return sessionId;
  }

  @Override
  public String name() {
    return primitiveName;
  }

  @Override
  public PrimitiveType serviceType() {
    return primitiveType;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public void addStateChangeListener(Consumer<State> listener) {
    stateChangeListeners.add(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<State> listener) {
    stateChangeListeners.remove(listener);
  }

  @Override
  public CompletableFuture<byte[]> execute(PrimitiveOperation operation) {
    CompletableFuture<byte[]> future = new CompletableFuture<>();
    threadContext.execute(() -> {
      if (primary == null) {
        future.completeExceptionally(new Unavailable());
        return;
      }

      ExecuteRequest request = new ExecuteRequest(sessionId.id(), operation);
      communicationService.<ExecuteRequest, ExecuteResponse>sendAndReceive(
          executeSubject,
          request,
          SERIALIZER::encode,
          SERIALIZER::decode,
          primary)
          .whenCompleteAsync((response, error) -> {
            if (error == null) {
              if (response.status() == Status.OK) {
                future.complete(response.result());
              } else {
                future.completeExceptionally(new Unavailable());
              }
            } else {
              future.completeExceptionally(error);
            }
          }, threadContext);
    });
    return future;
  }

  @Override
  public void addEventListener(Consumer<PrimitiveEvent> listener) {
    eventListeners.add(listener);
  }

  @Override
  public void removeEventListener(Consumer<PrimitiveEvent> listener) {
    eventListeners.remove(listener);
  }

  /**
   * Handles a replica change event.
   */
  private void handleReplicaChange(ReplicaInfo replicaInfo) {
    threadContext.execute(() -> {
      primary = replicaInfo.primary();
    });
  }

  /**
   * Handles a cluster event.
   */
  private void handleClusterEvent(ClusterEvent event) {
    if (event.type() == ClusterEvent.Type.NODE_DEACTIVATED) {
      threadContext.execute(() -> {
        state = State.SUSPENDED;
        stateChangeListeners.forEach(l -> l.accept(state));
      });
    }
  }

  /**
   * Handles a primitive event.
   */
  private void handleEvent(PrimitiveEvent event) {
    eventListeners.forEach(l -> l.accept(event));
  }

  @Override
  public CompletableFuture<PrimitiveProxy> open() {
    CompletableFuture<PrimitiveProxy> future = new CompletableFuture<>();
    OpenSessionRequest request = new OpenSessionRequest(
        clusterService.getLocalNode().id(),
        primitiveName,
        primitiveType.id());
    communicationService.<OpenSessionRequest, OpenSessionResponse>sendAndReceive(
        openSessionSubject,
        request,
        SERIALIZER::encode,
        SERIALIZER::decode,
        primary)
        .whenCompleteAsync((response, error) -> {
          if (error == null) {
            if (response.status() == Status.OK) {
              synchronized (this) {
                sessionId = SessionId.from(response.sessionId());
                state = State.CONNECTED;
              }
              executeSubject = new MessageSubject(String.format("%s-%s-execute", clientName, primitiveName));
              eventSubject = new MessageSubject(String.format("%s-%s-%s", clientName, primitiveName, sessionId));
              communicationService.addSubscriber(eventSubject, SERIALIZER::decode, this::handleEvent, threadContext);
              replicaProvider.addChangeListener(replicaChangeListener);
              clusterService.addListener(clusterEventListener);
              this.primary = replicaProvider.replicas().primary();
              stateChangeListeners.forEach(l -> l.accept(state));
              future.complete(this);
            } else {
              future.completeExceptionally(new Unavailable());
            }
          } else {
            future.completeExceptionally(error);
          }
        }, threadContext);
    return future;
  }

  @Override
  public boolean isOpen() {
    return state != State.CLOSED;
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    threadContext.execute(() -> {
      if (sessionId != null) {
        CloseSessionRequest request = new CloseSessionRequest(
            primitiveName,
            sessionId.id());
        communicationService.<CloseSessionRequest, CloseSessionResponse>sendAndReceive(
            closeSessionSubject,
            request,
            SERIALIZER::encode,
            SERIALIZER::decode,
            primary)
            .whenCompleteAsync((response, error) -> {
              if (error == null) {
                if (response.status() == Status.OK) {
                  replicaProvider.removeChangeListener(replicaChangeListener);
                  clusterService.removeListener(clusterEventListener);
                  if (eventSubject != null) {
                    communicationService.removeSubscriber(eventSubject);
                  }
                  state = State.CLOSED;
                  stateChangeListeners.forEach(l -> l.accept(state));
                } else {
                  future.completeExceptionally(new Unavailable());
                }
              } else {
                future.completeExceptionally(error);
              }
            }, threadContext);
      } else {
        future.complete(null);
      }
    });
    return future;
  }

  @Override
  public boolean isClosed() {
    return state == State.CLOSED;
  }
}
