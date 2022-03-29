// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set.impl;

import io.atomix.core.collection.impl.UnmodifiableAsyncDistributedCollection;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Unmodifiable distributed set.
 */
public class UnmodifiableAsyncDistributedSet<E> extends UnmodifiableAsyncDistributedCollection<E> implements AsyncDistributedSet<E> {
  private static final String ERROR_MSG = "updates are not allowed";

  public UnmodifiableAsyncDistributedSet(AsyncDistributedSet<E> delegateCollection) {
    super(delegateCollection);
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<E>> transactionLog) {
    throw new UnsupportedOperationException(ERROR_MSG);
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    throw new UnsupportedOperationException(ERROR_MSG);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    throw new UnsupportedOperationException(ERROR_MSG);
  }

  @Override
  public DistributedSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }
}
