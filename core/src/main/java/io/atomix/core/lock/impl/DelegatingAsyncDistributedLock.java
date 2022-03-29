// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.lock.impl;

import io.atomix.core.lock.AsyncAtomicLock;
import io.atomix.core.lock.AsyncDistributedLock;
import io.atomix.core.lock.DistributedLock;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous distributed lock that delegates to an {@link AsyncAtomicLock}.
 */
public class DelegatingAsyncDistributedLock extends DelegatingAsyncPrimitive<AsyncAtomicLock> implements AsyncDistributedLock {
  public DelegatingAsyncDistributedLock(AsyncAtomicLock atomicLock) {
    super(atomicLock);
  }

  @Override
  public CompletableFuture<Boolean> isLocked() {
    return delegate().isLocked();
  }

  @Override
  public CompletableFuture<Void> lock() {
    return delegate().lock().thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Boolean> tryLock() {
    return delegate().tryLock().thenApply(result -> result.isPresent());
  }

  @Override
  public CompletableFuture<Boolean> tryLock(Duration timeout) {
    return delegate().tryLock(timeout).thenApply(result -> result.isPresent());
  }

  @Override
  public CompletableFuture<Void> unlock() {
    return delegate().unlock();
  }

  @Override
  public DistributedLock sync(Duration operationTimeout) {
    return new BlockingDistributedLock(this, operationTimeout.toMillis());
  }
}
