// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.set.impl;

import io.atomix.core.cache.CacheConfig;
import io.atomix.core.collection.impl.CachingAsyncDistributedCollection;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Caching asynchronous distributed queue.
 */
public class CachingAsyncDistributedSet<E> extends CachingAsyncDistributedCollection<E> implements AsyncDistributedSet<E> {
  private final AsyncDistributedSet<E> backingSet;

  public CachingAsyncDistributedSet(AsyncDistributedSet<E> backingCollection, CacheConfig cacheConfig) {
    super(backingCollection, cacheConfig);
    this.backingSet = backingCollection;
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<SetUpdate<E>> transactionLog) {
    return backingSet.prepare(transactionLog);
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return backingSet.commit(transactionId);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return backingSet.rollback(transactionId);
  }

  @Override
  public DistributedSet<E> sync(Duration operationTimeout) {
    return new BlockingDistributedSet<>(this, operationTimeout.toMillis());
  }
}
