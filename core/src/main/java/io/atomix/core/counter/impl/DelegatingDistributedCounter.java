// SPDX-FileCopyrightText: 2018-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.counter.impl;

import io.atomix.core.counter.AsyncAtomicCounter;
import io.atomix.core.counter.AsyncDistributedCounter;
import io.atomix.core.counter.DistributedCounter;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Delegating distributed counter.
 */
public class DelegatingDistributedCounter extends DelegatingAsyncPrimitive<AsyncAtomicCounter> implements AsyncDistributedCounter {
  public DelegatingDistributedCounter(AsyncAtomicCounter primitive) {
    super(primitive);
  }

  @Override
  public CompletableFuture<Long> incrementAndGet() {
    return delegate().incrementAndGet();
  }

  @Override
  public CompletableFuture<Long> decrementAndGet() {
    return delegate().decrementAndGet();
  }

  @Override
  public CompletableFuture<Long> getAndIncrement() {
    return delegate().getAndIncrement();
  }

  @Override
  public CompletableFuture<Long> getAndDecrement() {
    return delegate().getAndDecrement();
  }

  @Override
  public CompletableFuture<Long> getAndAdd(long delta) {
    return delegate().getAndAdd(delta);
  }

  @Override
  public CompletableFuture<Long> addAndGet(long delta) {
    return delegate().addAndGet(delta);
  }

  @Override
  public CompletableFuture<Long> get() {
    return delegate().get();
  }

  @Override
  public DistributedCounter sync(Duration operationTimeout) {
    return new BlockingDistributedCounter(this, operationTimeout.toMillis());
  }
}
