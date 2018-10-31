/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.protocols.log.impl;

import com.google.common.collect.Sets;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.log.LogConsumer;
import io.atomix.primitive.log.LogProducer;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.log.Record;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.log.protocol.AppendRequest;
import io.atomix.protocols.log.protocol.LogClientProtocol;
import io.atomix.protocols.log.protocol.LogResponse;
import io.atomix.protocols.log.protocol.ReadRequest;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Distributed log session.
 */
public class DistributedLogSession implements LogSession {
  private final PartitionId partitionId;
  private final SessionId sessionId;
  private final LogClientProtocol protocol;
  private final ThreadContext threadContext;
  private final Set<Consumer<PrimitiveState>> stateChangeListeners = Sets.newIdentityHashSet();
  private final ClusterMembershipEventListener membershipEventListener = this::handleClusterEvent;
  private final PrimaryElectionEventListener primaryElectionListener = event -> changeReplicas(event.term());
  private final LogProducer producer = new DistributedLogProducer();
  private final LogConsumer consumer = new DistributedLogConsumer();
  private volatile PrimaryTerm term;
  private volatile PrimitiveState state = PrimitiveState.CLOSED;
  private final Logger log;

  public DistributedLogSession(
      PartitionId partitionId,
      SessionId sessionId,
      ClusterMembershipService clusterMembershipService,
      LogClientProtocol protocol,
      PrimaryElection primaryElection,
      ThreadContext threadContext) {
    this.partitionId = checkNotNull(partitionId, "partitionId cannot be null");
    this.sessionId = checkNotNull(sessionId, "sessionId cannot be null");
    this.protocol = checkNotNull(protocol, "protocol cannot be null");
    this.threadContext = checkNotNull(threadContext, "threadContext cannot be null");
    clusterMembershipService.addListener(membershipEventListener);
    primaryElection.addListener(primaryElectionListener);
    this.log = ContextualLoggerFactory.getLogger(getClass(), LoggerContext.builder(DistributedLogProducer.class)
        .addValue(partitionId.group() != null
            ? String.format("%s-%d", partitionId.group(), partitionId.id())
            : partitionId.id())
        .build());
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
  public ThreadContext context() {
    return threadContext;
  }

  @Override
  public PrimitiveState getState() {
    return state;
  }

  @Override
  public LogProducer producer() {
    return producer;
  }

  @Override
  public LogConsumer consumer() {
    return consumer;
  }

  /**
   * Handles a cluster event.
   */
  private void handleClusterEvent(ClusterMembershipEvent event) {
    PrimaryTerm term = this.term;
    if (term != null
        && event.type() == ClusterMembershipEvent.Type.MEMBER_REMOVED
        && event.subject().id().equals(term.primary().memberId())) {
      changeState(PrimitiveState.SUSPENDED);
    }
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
   * Changes the current session state.
   *
   * @param state the updated session state
   */
  private synchronized void changeState(PrimitiveState state) {
    if (this.state != state) {
      this.state = state;
      stateChangeListeners.forEach(l -> threadContext.execute(() -> l.accept(state)));
    }
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    stateChangeListeners.add(checkNotNull(listener));
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    stateChangeListeners.remove(checkNotNull(listener));
  }

  /**
   * Distributed log producer.
   */
  private class DistributedLogProducer implements LogProducer {
    @Override
    public CompletableFuture<Long> append(byte[] value) {
      CompletableFuture<Long> future = new CompletableFuture<>();
      PrimaryTerm term = DistributedLogSession.this.term;
      if (term != null && term.primary() != null) {
        protocol.append(term.primary().memberId(), AppendRequest.request(value))
            .whenCompleteAsync((response, error) -> {
              if (error == null) {
                if (response.status() == LogResponse.Status.OK) {
                  future.complete(response.index());
                } else {
                  future.completeExceptionally(new PrimitiveException.Unavailable());
                }
              } else {
                future.completeExceptionally(error);
              }
            }, threadContext);
      } else {
        future.completeExceptionally(new PrimitiveException.Unavailable());
      }
      return future;
    }
  }

  /**
   * Distributed log consumer.
   */
  private class DistributedLogConsumer implements LogConsumer {
    @Override
    public CompletableFuture<List<Record>> read(long index, int batchSize) {
      CompletableFuture<List<Record>> future = new CompletableFuture<>();
      PrimaryTerm term = DistributedLogSession.this.term;
      if (term != null && term.primary() != null) {
        protocol.read(term.primary().memberId(), ReadRequest.request(index, batchSize))
            .whenCompleteAsync((response, error) -> {
              if (error == null) {
                if (response.status() == LogResponse.Status.OK) {
                  future.complete(response.records());
                } else {
                  future.completeExceptionally(new PrimitiveException.Unavailable());
                }
              } else {
                future.completeExceptionally(error);
              }
            }, threadContext);
      } else {
        future.completeExceptionally(new PrimitiveException.Unavailable());
      }
      return future;
    }
  }
}
