// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction.impl;

import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.impl.MapUpdate;
import io.atomix.core.transaction.AsyncTransactionalMap;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionParticipant;
import io.atomix.core.transaction.TransactionalMap;
import io.atomix.primitive.PrimitiveType;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default transactional map.
 */
public abstract class TransactionalMapParticipant<K, V> implements AsyncTransactionalMap<K, V>, TransactionParticipant<MapUpdate<K, V>> {
  protected final TransactionId transactionId;
  protected final AsyncAtomicMap<K, V> consistentMap;

  protected TransactionalMapParticipant(TransactionId transactionId, AsyncAtomicMap<K, V> consistentMap) {
    this.transactionId = checkNotNull(transactionId);
    this.consistentMap = checkNotNull(consistentMap);
  }

  @Override
  public String name() {
    return consistentMap.name();
  }

  @Override
  public PrimitiveType type() {
    return consistentMap.type();
  }

  @Override
  public CompletableFuture<Boolean> prepare() {
    return consistentMap.prepare(log());
  }

  @Override
  public CompletableFuture<Void> commit() {
    return consistentMap.commit(transactionId);
  }

  @Override
  public CompletableFuture<Void> rollback() {
    return consistentMap.rollback(transactionId);
  }

  @Override
  public CompletableFuture<Void> close() {
    return consistentMap.close();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return consistentMap.delete();
  }

  @Override
  public TransactionalMap<K, V> sync(Duration operationTimeout) {
    return new BlockingTransactionalMap<>(this, operationTimeout.toMillis());
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
