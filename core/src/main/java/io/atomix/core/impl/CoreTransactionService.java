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

import io.atomix.core.map.AsyncConsistentMap;
import io.atomix.core.map.ConsistentMapType;
import io.atomix.core.transaction.ManagedTransactionService;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionService;
import io.atomix.core.transaction.TransactionState;
import io.atomix.primitive.PrimitiveManagementService;
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
  private final AtomicBoolean started = new AtomicBoolean();

  public CoreTransactionService(PrimitiveManagementService managementService) {
    this.managementService = checkNotNull(managementService);
  }

  @Override
  public Set<TransactionId> getActiveTransactions() {
    checkState(isRunning());
    return transactions.keySet().join();
  }

  @Override
  public TransactionState getTransactionState(TransactionId transactionId) {
    checkState(isRunning());
    return Versioned.valueOrNull(transactions.get(transactionId).join());
  }

  @Override
  public CompletableFuture<TransactionId> begin() {
    checkState(isRunning());
    TransactionId transactionId = TransactionId.from(UUID.randomUUID().toString());
    return transactions.put(transactionId, TransactionState.ACTIVE).thenApply(v -> transactionId);
  }

  @Override
  public CompletableFuture<Void> preparing(TransactionId transactionId) {
    checkState(isRunning());
    return transactions.put(transactionId, TransactionState.PREPARED).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> committing(TransactionId transactionId) {
    checkState(isRunning());
    return transactions.put(transactionId, TransactionState.COMMITTING).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> aborting(TransactionId transactionId) {
    checkState(isRunning());
    return transactions.put(transactionId, TransactionState.ROLLING_BACK).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> complete(TransactionId transactionId) {
    checkState(isRunning());
    return transactions.remove(transactionId).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<TransactionService> start() {
    return ConsistentMapType.<TransactionId, TransactionState>instance()
        .newPrimitiveBuilder("atomix-transactions", managementService)
        .withSerializer(SERIALIZER)
        .buildAsync()
        .thenApply(transactions -> {
          this.transactions = transactions.async();
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
    started.set(false);
    return CompletableFuture.completedFuture(null);
  }
}
