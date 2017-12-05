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
package io.atomix.impl;

import io.atomix.map.AsyncConsistentMap;
import io.atomix.map.ConsistentMapType;
import io.atomix.primitive.PrimitiveManagementService;
import io.atomix.transaction.ManagedTransactionService;
import io.atomix.transaction.TransactionId;
import io.atomix.transaction.TransactionService;
import io.atomix.transaction.TransactionState;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Core transaction service.
 */
public class CoreTransactionService implements ManagedTransactionService {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(TransactionId.class)
      .register(TransactionState.class)
      .build());
  private final PrimitiveManagementService managementService;
  private AsyncConsistentMap<TransactionId, TransactionState> transactions;
  private final AtomicBoolean open = new AtomicBoolean();

  public CoreTransactionService(PrimitiveManagementService managementService) {
    this.managementService = checkNotNull(managementService);
  }

  @Override
  public Set<TransactionId> getActiveTransactions() {
    checkState(isOpen());
    return transactions.keySet().join();
  }

  @Override
  public TransactionState getTransactionState(TransactionId transactionId) {
    checkState(isOpen());
    return Versioned.valueOrNull(transactions.get(transactionId).join());
  }

  @Override
  public CompletableFuture<TransactionId> begin() {
    checkState(isOpen());
    TransactionId transactionId = TransactionId.from(UUID.randomUUID().toString());
    return transactions.put(transactionId, TransactionState.ACTIVE).thenApply(v -> transactionId);
  }

  @Override
  public CompletableFuture<Void> preparing(TransactionId transactionId) {
    checkState(isOpen());
    return transactions.put(transactionId, TransactionState.PREPARED).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> committing(TransactionId transactionId) {
    checkState(isOpen());
    return transactions.put(transactionId, TransactionState.COMMITTING).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> aborting(TransactionId transactionId) {
    checkState(isOpen());
    return transactions.put(transactionId, TransactionState.ROLLING_BACK).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> complete(TransactionId transactionId) {
    checkState(isOpen());
    return transactions.remove(transactionId).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<TransactionService> open() {
    this.transactions = ConsistentMapType.<TransactionId, TransactionState>instance()
        .newPrimitiveBuilder("atomix-transactions", managementService)
        .withSerializer(SERIALIZER)
        .buildAsync();
    open.set(true);
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open.get();
  }

  @Override
  public CompletableFuture<Void> close() {
    open.set(false);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open.get();
  }
}
