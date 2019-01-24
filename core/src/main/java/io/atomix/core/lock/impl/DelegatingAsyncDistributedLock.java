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
