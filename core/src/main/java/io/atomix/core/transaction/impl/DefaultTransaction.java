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
package io.atomix.core.transaction.impl;

import com.google.common.collect.Sets;

import io.atomix.core.transaction.AsyncTransaction;
import io.atomix.core.transaction.CommitStatus;
import io.atomix.core.transaction.Isolation;
import io.atomix.core.transaction.Transaction;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionParticipant;
import io.atomix.core.transaction.TransactionService;
import io.atomix.core.transaction.TransactionalMapBuilder;
import io.atomix.core.transaction.TransactionalSetBuilder;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.utils.concurrent.Futures;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Default asynchronous transaction.
 */
public class DefaultTransaction implements AsyncTransaction {
  private volatile TransactionId transactionId;
  private final TransactionService transactionService;
  private final PrimitiveManagementService managementService;
  private final Isolation isolation;
  private final Set<TransactionParticipant<?>> participants = Sets.newCopyOnWriteArraySet();

  public DefaultTransaction(TransactionService transactionService, PrimitiveManagementService managementService, Isolation isolation) {
    this.transactionService = checkNotNull(transactionService);
    this.managementService = checkNotNull(managementService);
    this.isolation = checkNotNull(isolation);
  }

  @Override
  public String name() {
    return null;
  }

  @Override
  public TransactionId transactionId() {
    return transactionId;
  }

  @Override
  public Isolation isolation() {
    return isolation;
  }

  @Override
  public boolean isOpen() {
    return transactionId != null;
  }

  @Override
  public CompletableFuture<Void> begin() {
    return transactionService.begin().thenApply(transactionId -> {
      this.transactionId = transactionId;
      return null;
    });
  }

  void addParticipants(TransactionParticipant<?>... participants) {
    addParticipants(Arrays.asList(participants));
  }

  void addParticipants(Collection<TransactionParticipant<?>> participants) {
    this.participants.addAll(participants);
  }

  @Override
  public CompletableFuture<CommitStatus> commit() {
    Set<TransactionParticipant<?>> participants = this.participants.stream()
        .filter(p -> !p.log().records().isEmpty())
        .collect(Collectors.toSet());
    CompletableFuture<CommitStatus> status = transactionService.preparing(transactionId)
        .thenCompose(v -> prepare(participants))
        .thenCompose(result -> result
            ? transactionService.committing(transactionId)
            .thenCompose(v -> commit(participants))
            .thenApply(v -> CommitStatus.SUCCESS)
            : transactionService.aborting(transactionId)
            .thenCompose(v -> rollback(participants))
            .thenApply(v -> CommitStatus.FAILURE));
    return status.thenCompose(v -> transactionService.complete(transactionId).thenApply(u -> v));
  }

  private CompletableFuture<Boolean> prepare(Set<TransactionParticipant<?>> participants) {
    return Futures.allOf(participants.stream()
        .map(TransactionParticipant::prepare)
        .collect(Collectors.toList()))
        .thenApply(list -> list.stream().reduce(Boolean::logicalAnd).orElse(true));
  }

  private CompletableFuture<Void> commit(Set<TransactionParticipant<?>> participants) {
    return CompletableFuture.allOf(participants.stream()
        .map(TransactionParticipant::commit)
        .toArray(CompletableFuture[]::new));
  }

  private CompletableFuture<Void> rollback(Set<TransactionParticipant<?>> participants) {
    return CompletableFuture.allOf(participants.stream()
        .map(TransactionParticipant::rollback)
        .toArray(CompletableFuture[]::new));
  }

  @Override
  public CompletableFuture<Void> abort() {
    TransactionId transactionId = this.transactionId;
    if (transactionId == null) {
      return CompletableFuture.completedFuture(null);
    }
    return transactionService.complete(transactionId);
  }

  @Override
  public <K, V> TransactionalMapBuilder<K, V> mapBuilder(String name) {
    checkState(isOpen(), "transaction not open");
    return new DefaultTransactionalMapBuilder<>(name, managementService, this);
  }

  @Override
  public <E> TransactionalSetBuilder<E> setBuilder(String name) {
    checkState(isOpen(), "transaction not open");
    return new DefaultTransactionalSetBuilder<>(name, managementService, this);
  }

  @Override
  public CompletableFuture<Void> close() {
    return abort();
  }

  @Override
  public Transaction sync(Duration operationTimeout) {
    return new BlockingTransaction(this, operationTimeout.toMillis());
  }
}
