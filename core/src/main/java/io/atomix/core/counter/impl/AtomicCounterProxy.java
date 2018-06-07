/*
 * Copyright 2016-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.counter.impl;

import io.atomix.core.counter.AsyncAtomicCounter;
import io.atomix.core.counter.AtomicCounter;
import io.atomix.primitive.AbstractAsyncPrimitiveProxy;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Atomix counter implementation.
 */
public class AtomicCounterProxy extends AbstractAsyncPrimitiveProxy<AsyncAtomicCounter, AtomicCounterService> implements AsyncAtomicCounter {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .build());

  public AtomicCounterProxy(PrimitiveProxy proxy, PrimitiveRegistry registry) {
    super(AtomicCounterService.class, proxy, registry);
  }

  @Override
  public CompletableFuture<Long> get() {
    return applyBy(getPartitionKey(), service -> service.get());
  }

  @Override
  public CompletableFuture<Void> set(long value) {
    return acceptBy(getPartitionKey(), service -> service.set(value));
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(long expectedValue, long updateValue) {
    return applyBy(getPartitionKey(), service -> service.compareAndSet(expectedValue, updateValue));
  }

  @Override
  public CompletableFuture<Long> addAndGet(long delta) {
    return applyBy(getPartitionKey(), service -> service.addAndGet(delta));
  }

  @Override
  public CompletableFuture<Long> getAndAdd(long delta) {
    return applyBy(getPartitionKey(), service -> service.getAndAdd(delta));
  }

  @Override
  public CompletableFuture<Long> incrementAndGet() {
    return applyBy(getPartitionKey(), service -> service.incrementAndGet());
  }

  @Override
  public CompletableFuture<Long> getAndIncrement() {
    return applyBy(getPartitionKey(), service -> service.getAndIncrement());
  }

  @Override
  public CompletableFuture<Long> decrementAndGet() {
    return applyBy(getPartitionKey(), service -> service.decrementAndGet());
  }

  @Override
  public CompletableFuture<Long> getAndDecrement() {
    return applyBy(getPartitionKey(), service -> service.getAndDecrement());
  }

  @Override
  public AtomicCounter sync(Duration operationTimeout) {
    return new BlockingAtomicCounter(this, operationTimeout.toMillis());
  }
}