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

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.primitive.Consistency;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.Recovery;
import io.atomix.primitive.Replication;
import io.atomix.primitive.event.EventType;
import io.atomix.primitive.event.PrimitiveEvent;
import io.atomix.primitive.operation.PrimitiveOperation;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.proxy.PartitionProxy;
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

import java.net.ConnectException;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Primary-backup proxy.
 */
public class PrimaryBackupProxy implements PartitionProxy {
  private static final int RETRY_DELAY = 100;
  private Logger log;
  private final PrimitiveType primitiveType;
  private final PrimitiveDescriptor descriptor;
  private final ClusterMembershipService clusterMembershipService;
  private final PrimaryBackupClientProtocol protocol;
  private final PartitionId partitionId;
  private final SessionId sessionId;
  private final PrimaryElection primaryElection;
  private final ThreadContext threadContext;
  private final Set<Consumer<State>> stateChangeListeners = Sets.newIdentityHashSet();
  private final Map<EventType, Set<Consumer<PrimitiveEvent>>> eventListeners = Maps.newHashMap();
  private final PrimaryElectionEventListener primaryElectionListener = event -> changeReplicas(event.term());
  private final ClusterMembershipEventListener membershipEventListener = this::handleClusterEvent;
  private PrimaryTerm term;
  private volatile State state = State.CLOSED;

  public PrimaryBackupProxy(
      String clientName,
      PartitionId partitionId,
      SessionId sessionId,
      PrimitiveType primitiveType,
      PrimitiveDescriptor descriptor,
      ClusterMembershipService clusterMembershipService,
      PrimaryBackupClientProtocol protocol,
      PrimaryElection primaryElection,
      ThreadContext threadContext) {
    this.partitionId = checkNotNull(partitionId);
    this.sessionId = checkNotNull(sessionId);
    this.primitiveType = primitiveType;
    this.descriptor = descriptor;
    this.clusterMembershipService = clusterMembershipService;
    this.protocol = protocol;
    this.primaryElection = primaryElection;
    this.threadContext = threadContext;
    primaryElection.addListener(primaryElectionListener);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(PartitionProxy.class)
        .addValue(clientName)
        .add("type", primitiveType.id())
        .add("name", descriptor.name())
        .build());
  }

  @Override
  public String name() {
    return descriptor.name();
  }

  @Override
  public PrimitiveType type() {
    return primitiveType;
  }

  @Override
  public State getState() {
    return state;
  }

  @Override
  public PartitionId partitionId() {
    return partitionId;
  }

  @Override
  public SessionId sessionId() {
    return sessionId;
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
    ExecuteRequest request = ExecuteRequest.request(descriptor, sessionId.id(), clusterMembershipService.getLocalMember().id(), operation);
    log.trace("Sending {} to {}", request, term.primary());
    PrimaryTerm term = this.term;
    if (term.primary() != null) {
      protocol.execute(term.primary().memberId(), request).whenCompleteAsync((response, error) -> {
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
          Throwable cause = Throwables.getRootCause(error);
          if (cause instanceof PrimitiveException.Unavailable) {
            threadContext.schedule(Duration.ofMillis(RETRY_DELAY), () -> execute(operation, future));
          } else {
            future.completeExceptionally(error);
          }
        }
      }, threadContext);
    } else {
      future.completeExceptionally(new ConnectException());
    }
  }

  @Override
  public void addEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    eventListeners.computeIfAbsent(eventType.canonicalize(), t -> Sets.newLinkedHashSet()).add(listener);
  }

  @Override
  public void removeEventListener(EventType eventType, Consumer<PrimitiveEvent> listener) {
    eventListeners.computeIfAbsent(eventType.canonicalize(), t -> Sets.newLinkedHashSet()).remove(listener);
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
  private void handleClusterEvent(ClusterMembershipEvent event) {
    if (event.type() == ClusterMembershipEvent.Type.MEMBER_DEACTIVATED && event.subject().id().equals(term.primary().memberId())) {
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
    Set<Consumer<PrimitiveEvent>> listeners = eventListeners.get(event.type());
    if (listeners != null) {
      listeners.forEach(l -> l.accept(event));
    }
  }

  @Override
  public CompletableFuture<PartitionProxy> connect() {
    CompletableFuture<PartitionProxy> future = new CompletableFuture<>();
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
      protocol.close(term.primary().memberId(), new CloseRequest(descriptor, sessionId.id()))
          .whenCompleteAsync((response, error) -> {
            protocol.unregisterEventListener(sessionId);
            clusterMembershipService.removeListener(membershipEventListener);
            primaryElection.removeListener(primaryElectionListener);
            future.complete(null);
          }, threadContext);
    } else {
      future.complete(null);
    }
    return future;
  }

  /**
   * Primary-backup partition proxy builder.
   */
  public abstract static class Builder extends PartitionProxy.Builder {
    protected Consistency consistency = Consistency.SEQUENTIAL;
    protected Replication replication = Replication.ASYNCHRONOUS;
    protected Recovery recovery = Recovery.RECOVER;
    protected int numBackups = 1;
    protected int maxRetries = 0;
    protected Duration retryDelay = Duration.ofMillis(100);
    protected Executor executor;

    /**
     * Sets the protocol consistency model.
     *
     * @param consistency the protocol consistency model
     * @return the protocol builder
     */
    public Builder withConsistency(Consistency consistency) {
      this.consistency = checkNotNull(consistency, "consistency cannot be null");
      return this;
    }

    /**
     * Sets the protocol replication strategy.
     *
     * @param replication the protocol replication strategy
     * @return the protocol builder
     */
    public Builder withReplication(Replication replication) {
      this.replication = checkNotNull(replication, "replication cannot be null");
      return this;
    }

    /**
     * Sets the protocol recovery strategy.
     *
     * @param recovery the protocol recovery strategy
     * @return the protocol builder
     */
    public Builder withRecovery(Recovery recovery) {
      this.recovery = checkNotNull(recovery, "recovery cannot be null");
      return this;
    }

    /**
     * Sets the number of backups.
     *
     * @param numBackups the number of backups
     * @return the protocol builder
     */
    public Builder withNumBackups(int numBackups) {
      checkArgument(numBackups >= 0, "numBackups must be positive");
      this.numBackups = numBackups;
      return this;
    }

    /**
     * Sets the maximum number of retries before an operation can be failed.
     *
     * @param maxRetries the maximum number of retries before an operation can be failed
     * @return the proxy builder
     */
    public Builder withMaxRetries(int maxRetries) {
      checkArgument(maxRetries >= 0, "maxRetries must be positive");
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelayMillis the delay between operation retries in milliseconds
     * @return the proxy builder
     */
    public Builder withRetryDelayMillis(long retryDelayMillis) {
      return withRetryDelay(Duration.ofMillis(retryDelayMillis));
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelay the delay between operation retries
     * @param timeUnit   the delay time unit
     * @return the proxy builder
     * @throws NullPointerException if the time unit is null
     */
    public Builder withRetryDelay(long retryDelay, TimeUnit timeUnit) {
      return withRetryDelay(Duration.ofMillis(timeUnit.toMillis(retryDelay)));
    }

    /**
     * Sets the operation retry delay.
     *
     * @param retryDelay the delay between operation retries
     * @return the proxy builder
     * @throws NullPointerException if the delay is null
     */
    public Builder withRetryDelay(Duration retryDelay) {
      this.retryDelay = checkNotNull(retryDelay, "retryDelay cannot be null");
      return this;
    }

    /**
     * Sets the executor with which to complete proxy futures.
     *
     * @param executor The executor with which to complete proxy futures.
     * @return The proxy builder.
     * @throws NullPointerException if the executor is null
     */
    public Builder withExecutor(Executor executor) {
      this.executor = executor;
      return this;
    }
  }
}
