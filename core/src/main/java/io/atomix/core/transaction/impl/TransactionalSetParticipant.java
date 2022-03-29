// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.transaction.impl;

import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.impl.SetUpdate;
import io.atomix.core.transaction.AsyncTransactionalSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionParticipant;
import io.atomix.core.transaction.TransactionalSet;
import io.atomix.primitive.PrimitiveType;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default transactional map.
 */
public abstract class TransactionalSetParticipant<E> implements AsyncTransactionalSet<E>, TransactionParticipant<SetUpdate<E>> {
  protected final TransactionId transactionId;
  protected final AsyncDistributedSet<E> set;

  protected TransactionalSetParticipant(TransactionId transactionId, AsyncDistributedSet<E> set) {
    this.transactionId = checkNotNull(transactionId);
    this.set = checkNotNull(set);
  }

  @Override
  public String name() {
    return set.name();
  }

  @Override
  public PrimitiveType type() {
    return set.type();
  }

  @Override
  public CompletableFuture<Boolean> prepare() {
    return set.prepare(log());
  }

  @Override
  public CompletableFuture<Void> commit() {
    return set.commit(transactionId);
  }

  @Override
  public CompletableFuture<Void> rollback() {
    return set.rollback(transactionId);
  }

  @Override
  public CompletableFuture<Void> close() {
    return set.close();
  }

  @Override
  public CompletableFuture<Void> delete() {
    return set.delete();
  }

  @Override
  public TransactionalSet<E> sync(Duration operationTimeout) {
    return new BlockingTransactionalSet<>(this, operationTimeout.toMillis());
  }

  @Override
  public String toString() {
    return toStringHelper(this).toString();
  }
}
