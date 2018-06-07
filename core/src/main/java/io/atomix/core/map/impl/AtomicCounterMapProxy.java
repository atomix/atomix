/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.map.impl;

import io.atomix.core.map.AsyncAtomicCounterMap;
import io.atomix.core.map.AtomicCounterMap;
import io.atomix.primitive.AbstractAsyncPrimitiveProxy;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.proxy.PrimitiveProxy;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;

/**
 * {@code AsyncAtomicCounterMap} implementation backed by Atomix.
 */
public class AtomicCounterMapProxy extends AbstractAsyncPrimitiveProxy<AsyncAtomicCounterMap<String>, AtomicCounterMapService> implements AsyncAtomicCounterMap<String> {
  public AtomicCounterMapProxy(PrimitiveProxy proxy, PrimitiveRegistry registry) {
    super(AtomicCounterMapService.class, proxy, registry);
  }

  @Override
  public CompletableFuture<Long> incrementAndGet(String key) {
    return applyBy(key, service -> service.incrementAndGet(key));
  }

  @Override
  public CompletableFuture<Long> decrementAndGet(String key) {
    return applyBy(key, service -> service.decrementAndGet(key));
  }

  @Override
  public CompletableFuture<Long> getAndIncrement(String key) {
    return applyBy(key, service -> service.getAndIncrement(key));
  }

  @Override
  public CompletableFuture<Long> getAndDecrement(String key) {
    return applyBy(key, service -> service.getAndDecrement(key));
  }

  @Override
  public CompletableFuture<Long> addAndGet(String key, long delta) {
    return applyBy(key, service -> service.addAndGet(key, delta));
  }

  @Override
  public CompletableFuture<Long> getAndAdd(String key, long delta) {
    return applyBy(key, service -> service.getAndAdd(key, delta));
  }

  @Override
  public CompletableFuture<Long> get(String key) {
    return applyBy(key, service -> service.get(key));
  }

  @Override
  public CompletableFuture<Long> put(String key, long newValue) {
    return applyBy(key, service -> service.put(key, newValue));
  }

  @Override
  public CompletableFuture<Long> putIfAbsent(String key, long newValue) {
    return applyBy(key, service -> service.putIfAbsent(key, newValue));
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, long expectedOldValue, long newValue) {
    return applyBy(key, service -> service.replace(key, expectedOldValue, newValue));
  }

  @Override
  public CompletableFuture<Long> remove(String key) {
    return applyBy(key, service -> service.remove(key));
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, long value) {
    return applyBy(key, service -> service.remove(key, value));
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return applyAll(service -> service.isEmpty())
        .thenApply(results -> results.allMatch(Predicate.isEqual(true)));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return applyAll(service -> service.size())
        .thenApply(results -> results.reduce(Math::addExact).orElse(0));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return acceptAll(service -> service.clear());
  }

  @Override
  public AtomicCounterMap<String> sync(Duration operationTimeout) {
    return new BlockingAtomicCounterMap<>(this, operationTimeout.toMillis());
  }
}