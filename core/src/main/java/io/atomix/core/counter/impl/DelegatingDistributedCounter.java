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
