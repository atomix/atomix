/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.semaphore.impl;

import io.atomix.core.semaphore.AsyncAtomicSemaphore;
import io.atomix.core.semaphore.AsyncDistributedSemaphore;
import io.atomix.core.semaphore.DistributedSemaphore;
import io.atomix.core.semaphore.DistributedSemaphoreType;
import io.atomix.core.semaphore.QueueStatus;
import io.atomix.primitive.PrimitiveType;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Delegating distributed semaphore.
 */
public class DelegatingAsyncDistributedSemaphore extends DelegatingAsyncPrimitive<AsyncAtomicSemaphore> implements AsyncDistributedSemaphore {
  public DelegatingAsyncDistributedSemaphore(AsyncAtomicSemaphore primitive) {
    super(primitive);
  }

  @Override
  public PrimitiveType type() {
    return DistributedSemaphoreType.instance();
  }

  @Override
  public CompletableFuture<Void> acquire() {
    return delegate().acquire().thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Void> acquire(int permits) {
    return delegate().acquire(permits).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<Boolean> tryAcquire() {
    return delegate().tryAcquire().thenApply(Optional::isPresent);
  }

  @Override
  public CompletableFuture<Boolean> tryAcquire(int permits) {
    return delegate().tryAcquire(permits).thenApply(Optional::isPresent);
  }

  @Override
  public CompletableFuture<Boolean> tryAcquire(Duration timeout) {
    return delegate().tryAcquire(timeout).thenApply(Optional::isPresent);
  }

  @Override
  public CompletableFuture<Boolean> tryAcquire(int permits, Duration timeout) {
    return delegate().tryAcquire(permits, timeout).thenApply(Optional::isPresent);
  }

  @Override
  public CompletableFuture<Void> release() {
    return delegate().release();
  }

  @Override
  public CompletableFuture<Void> release(int permits) {
    return delegate().release(permits);
  }

  @Override
  public CompletableFuture<Integer> availablePermits() {
    return delegate().availablePermits();
  }

  @Override
  public CompletableFuture<Integer> drainPermits() {
    return delegate().drainPermits();
  }

  @Override
  public CompletableFuture<Integer> increasePermits(int permits) {
    return delegate().increasePermits(permits);
  }

  @Override
  public CompletableFuture<Integer> reducePermits(int permits) {
    return delegate().reducePermits(permits);
  }

  @Override
  public CompletableFuture<QueueStatus> queueStatus() {
    return delegate().queueStatus();
  }

  @Override
  public DistributedSemaphore sync(Duration operationTimeout) {
    return new BlockingDistributedSemaphore(this, operationTimeout);
  }
}
