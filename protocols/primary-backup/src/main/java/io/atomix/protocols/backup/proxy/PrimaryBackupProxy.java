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
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.primitive.proxy.impl.AbstractPrimitiveProxy;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.backup.protocol.CloseRequest;
import io.atomix.protocols.backup.protocol.ExecuteRequest;
import io.atomix.protocols.backup.protocol.PrimaryBackupClientProtocol;
import io.atomix.protocols.backup.protocol.PrimaryBackupResponse.Status;
import io.atomix.protocols.backup.protocol.PrimitiveDescriptor;
import io.atomix.utils.concurrent.ComposableFuture;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary-backup proxy.
 */
public class PrimaryBackupProxy extends AbstractPrimitiveProxy {
  private Logger log;
  private final PrimitiveType primitiveType;
  private final PrimitiveDescriptor descriptor;
  private final ClusterService clusterService;
  private final PrimaryBackupClientProtocol protocol;
  private final SessionId sessionId;
  private final PrimaryElection primaryElection;
  private final ThreadContext threadContext;
  private final Set<Consumer<State>> stateChangeListeners = Sets.newIdentityHashSet();
  private final Set<Consumer<PrimitiveEvent>> eventListeners = Sets.newIdentityHashSet();
  private final PrimaryElectionEventListener primaryElectionListener = event -> changeReplicas(event.term());
  private final ClusterEventListener clusterEventListener = this::handleClusterEvent;
  private PrimaryTerm term;
  private volatile State state = State.CLOSED;

  public PrimaryBackupProxy(
      String clientName,
      SessionId sessionId,
      PrimitiveType primitiveType,
      PrimitiveDescriptor descriptor,
      ClusterService clusterService,
      PrimaryBackupClientProtocol protocol,
      PrimaryElection primaryElection,
      ThreadContext threadContext) {
    this.sessionId = checkNotNull(sessionId);
    this.primitiveType = primitiveType;
    this.descriptor = descriptor;
    this.clusterService = clusterService;
    this.protocol = protocol;
    this.primaryElection = primaryElection;
    this.threadContext = threadContext;
    primaryElection.addListener(primaryElectionListener);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PrimitiveProxy.class)
        .addValue(clientName)
        .add("type", primitiveType.id())
        .add("name", descriptor.name())
        .build());
  }

  @Override
  public SessionId sessionId() {
    return sessionId;
  }

  @Override
  public String name() {
    return descriptor.name();
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
    ComposableFuture<byte[]> future = new ComposableFuture<>();
    threadContext.execute(() -> {
      if (term.primary() == null) {
        primaryElection.getTerm().whenCompleteAsync((term, error) -> {
          if (error == null) {
            if (term.term() <= this.term.term() || term.primary() == null) {
              future.completeExceptionally(new PrimitiveException.Unavailable());
            } else {
              this.term = term;
              execute(operation, future);
            }
          } else {
            future.completeExceptionally(new PrimitiveException.Unavailable());
          }
        }, threadContext);
      } else {
        execute(operation, future);
      }
    });
    return future;
  }

  private void execute(PrimitiveOperation operation, ComposableFuture<byte[]> future) {
    ExecuteRequest request = ExecuteRequest.request(descriptor, sessionId.id(), clusterService.getLocalNode().id(), operation);
    log.trace("Sending {} to {}", request, term.primary());
    PrimaryTerm term = this.term;
    protocol.execute(term.primary(), request).whenCompleteAsync((response, error) -> {
      if (error == null) {
        log.trace("Received {}", response);
        if (response.status() == Status.OK) {
          future.complete(response.result());
        } else {
          if (this.term.term() > term.term()) {
            execute(operation).whenComplete(future);
          } else {
            primaryElection.getTerm().whenComplete((newTerm, termError) -> {
              if (termError == null) {
                if (newTerm.term() > term.term() && newTerm.primary() != null) {
                  execute(operation).whenComplete(future);
                } else {
                  future.completeExceptionally(new PrimitiveException.Unavailable());
                }
              } else {
                future.completeExceptionally(new PrimitiveException.Unavailable());
              }
            });
          }
        }
      } else {
        future.completeExceptionally(error);
      }
    }, threadContext);
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
  private void changeReplicas(PrimaryTerm term) {
    threadContext.execute(() -> {
      if (this.term == null || term.term() > this.term.term()) {
        this.term = term;
      }
    });
  }

  /**
   * Handles a cluster event.
   */
  private void handleClusterEvent(ClusterEvent event) {
    if (event.type() == ClusterEvent.Type.NODE_DEACTIVATED && event.subject().id().equals(term.primary())) {
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
    log.trace("Received {}", event);
    eventListeners.forEach(l -> l.accept(event));
  }

  @Override
  public CompletableFuture<PrimitiveProxy> connect() {
    CompletableFuture<PrimitiveProxy> future = new CompletableFuture<>();
    threadContext.execute(() -> {
      primaryElection.getTerm().whenCompleteAsync((term, error) -> {
        if (error == null) {
          if (term.primary() == null) {
            future.completeExceptionally(new PrimitiveException.Unavailable());
          } else {
            this.term = term;
            protocol.registerEventListener(sessionId, this::handleEvent, threadContext);
            future.complete(this);
          }
        } else {
          future.completeExceptionally(new PrimitiveException.Unavailable());
        }
      }, threadContext);
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    if (term.primary() != null) {
      protocol.close(term.primary(), new CloseRequest(descriptor, sessionId.id()))
          .whenCompleteAsync((response, error) -> {
            protocol.unregisterEventListener(sessionId);
            clusterService.removeListener(clusterEventListener);
            primaryElection.removeListener(primaryElectionListener);
            future.complete(null);
          }, threadContext);
    } else {
      future.complete(null);
    }
    return future;
  }
}
