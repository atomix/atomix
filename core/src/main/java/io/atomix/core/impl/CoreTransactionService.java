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
package io.atomix.core.impl;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import io.atomix.cluster.ClusterMembershipEvent;
import io.atomix.cluster.ClusterMembershipEventListener;
import io.atomix.cluster.MemberId;
import io.atomix.core.iterator.AsyncIterator;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMapConfig;
import io.atomix.core.map.AtomicMapType;
import io.atomix.core.transaction.ManagedTransactionService;
import io.atomix.core.transaction.ParticipantInfo;
import io.atomix.core.transaction.TransactionException;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionService;
import io.atomix.core.transaction.TransactionState;
import io.atomix.core.transaction.Transactional;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.primitive.PrimitiveBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.partition.PartitionGroup;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.primitive.protocol.ProxyCompatibleBuilder;
import io.atomix.primitive.protocol.ProxyProtocol;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.serializer.Namespace;
import io.atomix.utils.serializer.Namespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Core transaction service.
 */
public class CoreTransactionService implements ManagedTransactionService {
  private static final Logger LOGGER = LoggerFactory.getLogger(CoreTransactionService.class);
  private static final Serializer SERIALIZER = Serializer.using(Namespace.builder()
      .register(Namespaces.BASIC)
      .register(MemberId.class)
      .register(TransactionId.class)
      .register(TransactionState.class)
      .register(ParticipantInfo.class)
      .register(TransactionInfo.class)
      .build());
  private final PrimitiveManagementService managementService;
  private final MemberId localMemberId;
  private final ClusterMembershipEventListener clusterEventListener = this::onMembershipChange;
  private volatile AsyncAtomicMap<TransactionId, TransactionInfo> transactions;
  private final AtomicBoolean started = new AtomicBoolean();

  public CoreTransactionService(PrimitiveManagementService managementService) {
    this.managementService = checkNotNull(managementService);
    this.localMemberId = managementService.getMembershipService().getLocalMember().id();
  }

  @Override
  public Set<TransactionId> getActiveTransactions() {
    checkState(isRunning());
    return transactions.sync().keySet();
  }

  @Override
  public TransactionState getTransactionState(TransactionId transactionId) {
    checkState(isRunning());
    TransactionInfo info = Versioned.valueOrNull(transactions.get(transactionId).join());
    return info != null ? info.state : null;
  }

  @Override
  public CompletableFuture<TransactionId> begin() {
    checkState(isRunning());
    TransactionId transactionId = TransactionId.from(UUID.randomUUID().toString());
    TransactionInfo info = new TransactionInfo(localMemberId, TransactionState.ACTIVE, ImmutableSet.of());
    return transactions.put(transactionId, info).thenApply(v -> transactionId);
  }

  @Override
  public CompletableFuture<Void> preparing(TransactionId transactionId, Set<ParticipantInfo> participants) {
    checkState(isRunning());
    return transactions.compute(transactionId, (id, info) -> {
      if (info == null) {
        return null;
      } else if (info.state == TransactionState.ACTIVE && info.coordinator.equals(localMemberId)) {
        return new TransactionInfo(info.coordinator, TransactionState.PREPARING, participants);
      } else {
        return info;
      }
    }).thenCompose(value -> {
      if (value == null || value.value() == null) {
        return Futures.exceptionalFuture(new TransactionException("Unknown transaction " + transactionId));
      } else if (value.value().state != TransactionState.PREPARING) {
        return Futures.exceptionalFuture(new TransactionException("Concurrent transaction modification " + transactionId));
      } else if (!value.value().coordinator.equals(localMemberId)) {
        return Futures.exceptionalFuture(new TransactionException("Transaction " + transactionId + " recovered by another member"));
      }
      return Futures.completedFuture(null);
    });
  }

  @Override
  public CompletableFuture<Void> committing(TransactionId transactionId) {
    checkState(isRunning());
    return transactions.compute(transactionId, (id, info) -> {
      if (info == null) {
        return null;
      } else if (info.state == TransactionState.PREPARING && info.coordinator.equals(localMemberId)) {
        return new TransactionInfo(info.coordinator, TransactionState.COMMITTING, info.participants);
      } else {
        return info;
      }
    }).thenCompose(value -> {
      if (value == null || value.value() == null) {
        return Futures.exceptionalFuture(new TransactionException("Unknown transaction " + transactionId));
      } else if (value.value().state != TransactionState.COMMITTING) {
        return Futures.exceptionalFuture(new TransactionException("Concurrent transaction modification " + transactionId));
      } else if (!value.value().coordinator.equals(localMemberId)) {
        return Futures.exceptionalFuture(new TransactionException("Transaction " + transactionId + " recovered by another member"));
      }
      return Futures.completedFuture(null);
    });
  }

  @Override
  public CompletableFuture<Void> aborting(TransactionId transactionId) {
    checkState(isRunning());
    return transactions.compute(transactionId, (id, info) -> {
      if (info == null) {
        return null;
      } else if (info.state == TransactionState.PREPARING && info.coordinator.equals(localMemberId)) {
        return new TransactionInfo(info.coordinator, TransactionState.ROLLING_BACK, info.participants);
      } else {
        return info;
      }
    }).thenCompose(value -> {
      if (value == null || value.value() == null) {
        return Futures.exceptionalFuture(new TransactionException("Unknown transaction " + transactionId));
      } else if (value.value().state != TransactionState.ROLLING_BACK) {
        return Futures.exceptionalFuture(new TransactionException("Concurrent transaction modification " + transactionId));
      } else if (!value.value().coordinator.equals(localMemberId)) {
        return Futures.exceptionalFuture(new TransactionException("Transaction " + transactionId + " recovered by another member"));
      }
      return Futures.completedFuture(null);
    });
  }

  @Override
  public CompletableFuture<Void> complete(TransactionId transactionId) {
    checkState(isRunning());
    return transactions.remove(transactionId).thenApply(v -> null);
  }

  /**
   * Handles a cluster membership change event.
   */
  private void onMembershipChange(ClusterMembershipEvent event) {
    if (event.type() == ClusterMembershipEvent.Type.MEMBER_REMOVED) {
      recoverTransactions(transactions.entrySet().iterator(), event.subject().id());
    }
  }

  /**
   * Recursively recovers transactions using the given iterator.
   *
   * @param iterator the asynchronous iterator from which to recover transactions
   * @param memberId the transaction member ID
   */
  private void recoverTransactions(AsyncIterator<Map.Entry<TransactionId, Versioned<TransactionInfo>>> iterator, MemberId memberId) {
    iterator.next().thenAccept(entry -> {
      if (entry.getValue().value().coordinator.equals(memberId)) {
        recoverTransaction(entry.getKey(), entry.getValue().value());
      }
      recoverTransactions(iterator, memberId);
    });
  }

  /**
   * Recovers and completes the given transaction.
   *
   * @param transactionId the transaction identifier
   * @param transactionInfo the transaction info
   */
  private void recoverTransaction(TransactionId transactionId, TransactionInfo transactionInfo) {
    switch (transactionInfo.state) {
      case PREPARING:
        completePreparingTransaction(transactionId);
        break;
      case COMMITTING:
        completeCommittingTransaction(transactionId);
        break;
      case ROLLING_BACK:
        completeRollingBackTransaction(transactionId);
        break;
      default:
        break;
    }
  }

  /**
   * Completes a transaction in the {@link TransactionState#PREPARING} state.
   *
   * @param transactionId the transaction identifier for the transaction to complete
   */
  private void completePreparingTransaction(TransactionId transactionId) {
    // Change ownership of the transaction to the local node and set its state to ROLLING_BACK and then roll it back.
    completeTransaction(
        transactionId,
        TransactionState.PREPARING,
        info -> new TransactionInfo(localMemberId, TransactionState.ROLLING_BACK, info.participants),
        info -> info.state == TransactionState.ROLLING_BACK,
        (id, transactional) -> transactional.rollback(id))
        .whenComplete((result, error) -> {
          if (error != null) {
            error = Throwables.getRootCause(error);
            if (error instanceof TransactionException) {
              LOGGER.warn("Failed to complete transaction", error);
            } else {
              LOGGER.warn("Failed to roll back transaction " + transactionId);
            }
          }
        });
  }

  /**
   * Completes a transaction in the {@link TransactionState#PREPARING} state.
   *
   * @param transactionId the transaction identifier for the transaction to complete
   */
  private void completeCommittingTransaction(TransactionId transactionId) {
    // Change ownership of the transaction to the local node and then commit it.
    completeTransaction(
        transactionId,
        TransactionState.COMMITTING,
        info -> new TransactionInfo(localMemberId, TransactionState.COMMITTING, info.participants),
        info -> info.state == TransactionState.COMMITTING && info.coordinator.equals(localMemberId),
        (id, transactional) -> transactional.commit(id))
        .whenComplete((result, error) -> {
          if (error != null) {
            error = Throwables.getRootCause(error);
            if (error instanceof TransactionException) {
              LOGGER.warn("Failed to complete transaction", error);
            } else {
              LOGGER.warn("Failed to commit transaction " + transactionId);
            }
          }
        });
  }

  /**
   * Completes a transaction in the {@link TransactionState#PREPARING} state.
   *
   * @param transactionId the transaction identifier for the transaction to complete
   */
  private void completeRollingBackTransaction(TransactionId transactionId) {
    // Change ownership of the transaction to the local node and then roll it back.
    completeTransaction(
        transactionId,
        TransactionState.ROLLING_BACK,
        info -> new TransactionInfo(localMemberId, TransactionState.ROLLING_BACK, info.participants),
        info -> info.state == TransactionState.ROLLING_BACK && info.coordinator.equals(localMemberId),
        (id, transactional) -> transactional.rollback(id))
        .whenComplete((result, error) -> {
          if (error != null) {
            error = Throwables.getRootCause(error);
            if (error instanceof TransactionException) {
              LOGGER.warn("Failed to complete transaction", error);
            } else {
              LOGGER.warn("Failed to roll back transaction " + transactionId);
            }
          }
        });
  }

  /**
   * Completes a transaction by modifying the transaction state to change ownership to this member and then completing
   * the transaction based on the existing transaction state.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Void> completeTransaction(
      TransactionId transactionId,
      TransactionState expectState,
      Function<TransactionInfo, TransactionInfo> updateFunction,
      Predicate<TransactionInfo> updatedPredicate,
      BiFunction<TransactionId, Transactional<?>, CompletableFuture<Void>> completionFunction) {
    return transactions.compute(transactionId, (id, info) -> {
      if (info == null) {
        return null;
      } else if (info.state == expectState) {
        return updateFunction.apply(info);
      } else {
        return info;
      }
    }).thenCompose(value -> {
      if (value != null && updatedPredicate.test(value.value())) {
        return Futures.allOf(value.value().participants.stream()
            .map(participantInfo -> completeParticipant(participantInfo, info -> completionFunction.apply(transactionId, info))))
            .thenApply(v -> null);
      }
      return Futures.exceptionalFuture(new TransactionException("Failed to acquire transaction lock"));
    });
  }

  /**
   * Completes an individual participant in a transaction by loading the primitive by type/protocol/partition group and
   * applying the given completion function to it.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Void> completeParticipant(
      ParticipantInfo participantInfo,
      Function<Transactional<?>, CompletableFuture<Void>> completionFunction) {
    // Look up the primitive type for the participant. If the primitive type is not found, return an exception.
    PrimitiveType primitiveType = managementService.getPrimitiveTypeRegistry().getPrimitiveType(participantInfo.type());
    if (primitiveType == null) {
      return Futures.exceptionalFuture(new TransactionException("Failed to locate primitive type " + participantInfo.type() + " for participant " + participantInfo.name()));
    }

    // Look up the protocol type for the participant.
    PrimitiveProtocol.Type protocolType = managementService.getProtocolTypeRegistry().getProtocolType(participantInfo.protocol());
    if (protocolType == null) {
      return Futures.exceptionalFuture(new TransactionException("Failed to locate protocol type for participant " + participantInfo.name()));
    }

    // Look up the partition group in which the primitive is stored.
    PartitionGroup partitionGroup;
    if (participantInfo.group() == null) {
      partitionGroup = managementService.getPartitionService().getPartitionGroup(protocolType);
    } else {
      partitionGroup = managementService.getPartitionService().getPartitionGroup(participantInfo.group());
    }

    // If the partition group is not found, return an exception.
    if (partitionGroup == null) {
      return Futures.exceptionalFuture(new TransactionException("Failed to locate partition group for participant " + participantInfo.name()));
    }

    PrimitiveBuilder builder = primitiveType.newBuilder(participantInfo.name(), primitiveType.newConfig(), managementService);
    ((ProxyCompatibleBuilder) builder).withProtocol(partitionGroup.newProtocol());
    DistributedPrimitive primitive = builder.build();
    return completionFunction.apply((Transactional<?>) primitive);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<TransactionService> start() {
    PrimitiveProtocol protocol = managementService.getPartitionService().getSystemPartitionGroup().newProtocol();
    return AtomicMapType.<TransactionId, TransactionInfo>instance()
        .newBuilder("atomix-transactions", new AtomicMapConfig(), managementService)
        .withSerializer(SERIALIZER)
        .withProtocol((ProxyProtocol) protocol)
        .withCacheEnabled()
        .buildAsync()
        .thenApply(transactions -> {
          this.transactions = transactions.async();
          managementService.getMembershipService().addListener(clusterEventListener);
          LOGGER.info("Started");
          started.set(true);
          return this;
        });
  }

  @Override
  public boolean isRunning() {
    return started.get();
  }

  @Override
  public CompletableFuture<Void> stop() {
    if (started.compareAndSet(true, false)) {
      managementService.getMembershipService().removeListener(clusterEventListener);
      return transactions.close().exceptionally(e -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Transaction info.
   */
  private static class TransactionInfo {
    private final MemberId coordinator;
    private final TransactionState state;
    private final Set<ParticipantInfo> participants;

    TransactionInfo(MemberId coordinator, TransactionState state, Set<ParticipantInfo> participants) {
      this.coordinator = coordinator;
      this.state = state;
      this.participants = participants;
    }
  }
}
