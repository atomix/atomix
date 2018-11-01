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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import com.google.common.collect.Sets;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.ClusterMembershipService;
import io.atomix.cluster.MemberId;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.log.LogConsumer;
import io.atomix.primitive.log.LogProducer;
import io.atomix.primitive.log.LogRecord;
import io.atomix.primitive.log.LogSession;
import io.atomix.primitive.partition.PartitionId;
import io.atomix.primitive.partition.PrimaryElection;
import io.atomix.primitive.partition.PrimaryElectionEventListener;
import io.atomix.primitive.partition.PrimaryTerm;
import io.atomix.primitive.session.SessionId;
import io.atomix.protocols.log.protocol.AppendRequest;
import io.atomix.protocols.log.protocol.ConsumeRequest;
import io.atomix.protocols.log.protocol.LogClientProtocol;
import io.atomix.protocols.log.protocol.LogResponse;
import io.atomix.protocols.log.protocol.RecordsRequest;
import io.atomix.protocols.log.protocol.ResetRequest;
import io.atomix.utils.concurrent.ThreadContext;
import io.atomix.utils.logging.ContextualLoggerFactory;
import io.atomix.utils.logging.LoggerContext;
import org.slf4j.Logger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Distributed log session.
 */
public class DistributedLogSession implements LogSession {
  private final PartitionId partitionId;
  private final SessionId sessionId;
  private final LogClientProtocol protocol;
  private final PrimaryElection primaryElection;
  private final ThreadContext threadContext;
  private final Set<Consumer<PrimitiveState>> stateChangeListeners = Sets.newIdentityHashSet();
  private final ClusterMembershipEventListener membershipEventListener = this::handleClusterEvent;
  private final PrimaryElectionEventListener primaryElectionListener = event -> changeReplicas(event.term());
  private final DistributedLogProducer producer = new DistributedLogProducer();
  private final DistributedLogConsumer consumer = new DistributedLogConsumer();
  private final MemberId memberId;
  private final String subject;
  private PrimaryTerm term;
  private volatile PrimitiveState state = PrimitiveState.CONNECTED;
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
    this.primaryElection = checkNotNull(primaryElection, "primaryElection cannot be null");
    this.threadContext = checkNotNull(threadContext, "threadContext cannot be null");
    this.memberId = clusterMembershipService.getLocalMember().id();
    this.subject = String.format("%s-%s-%s", partitionId.group(), partitionId.id(), sessionId);
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
        consumer.register(term.primary().memberId());
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

  @Override
  public CompletableFuture<LogSession> connect() {
    return term()
        .thenRun(() -> changeState(PrimitiveState.CONNECTED))
        .thenApply(v -> this);
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    threadContext.execute(() -> {
      changeState(PrimitiveState.CLOSED);
      future.complete(null);
    });
    return future;
  }

  /**
   * Returns the current primary term.
   *
   * @return the current primary term
   */
  private CompletableFuture<PrimaryTerm> term() {
    CompletableFuture<PrimaryTerm> future = new CompletableFuture<>();
    threadContext.execute(() -> {
      if (term != null) {
        future.complete(term);
      } else {
        primaryElection.getTerm().whenCompleteAsync((term, error) -> {
          if (term != null) {
            this.term = term;
            future.complete(term);
          } else {
            future.completeExceptionally(new PrimitiveException.Unavailable());
          }
        });
      }
    });
    return future;
  }

  /**
   * Distributed log producer.
   */
  private class DistributedLogProducer implements LogProducer {
    @Override
    public CompletableFuture<Long> append(byte[] value) {
      CompletableFuture<Long> future = new CompletableFuture<>();
      term().thenCompose(term -> protocol.append(term.primary().memberId(), AppendRequest.request(value)))
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
      return future;
    }
  }

  /**
   * Distributed log consumer.
   */
  private class DistributedLogConsumer implements LogConsumer {
    private MemberId leader;
    private long index;
    private volatile Consumer<LogRecord> consumer;

    /**
     * Registers the consumer with the given leader.
     *
     * @param leader the leader with which to register the consumer
     */
    private CompletableFuture<Void> register(MemberId leader) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      this.leader = leader;
      protocol.consume(leader, ConsumeRequest.request(memberId, subject, index + 1))
          .whenCompleteAsync((response, error) -> {
            if (error == null) {
              if (response.status() == LogResponse.Status.OK) {
                future.complete(null);
              } else {
                future.completeExceptionally(new PrimitiveException.Unavailable());
              }
            } else {
              future.completeExceptionally(error);
            }
          }, threadContext);
      return future;
    }

    /**
     * Handles a records request.
     *
     * @param request the request to handle
     */
    private void handleRecords(RecordsRequest request) {
      if (request.reset()) {
        index = request.record().index() - 1;
      }
      if (request.record().index() == index + 1) {
        Consumer<LogRecord> consumer = this.consumer;
        if (consumer != null) {
          consumer.accept(request.record());
          index = request.record().index();
        }
      } else {
        protocol.reset(leader, ResetRequest.request(memberId, subject, index + 1));
      }
    }

    @Override
    public CompletableFuture<Void> consume(long index, Consumer<LogRecord> consumer) {
      return term().thenCompose(term -> {
        protocol.registerRecordsConsumer(subject, this::handleRecords, threadContext);
        this.consumer = consumer;
        this.index = index - 1;
        return register(term.primary().memberId());
      });
    }
  }
}
