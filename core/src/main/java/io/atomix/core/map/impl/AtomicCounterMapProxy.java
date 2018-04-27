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
import io.atomix.core.map.impl.AtomicCounterMapOperations.AddAndGet;
import io.atomix.core.map.impl.AtomicCounterMapOperations.DecrementAndGet;
import io.atomix.core.map.impl.AtomicCounterMapOperations.Get;
import io.atomix.core.map.impl.AtomicCounterMapOperations.GetAndAdd;
import io.atomix.core.map.impl.AtomicCounterMapOperations.GetAndDecrement;
import io.atomix.core.map.impl.AtomicCounterMapOperations.GetAndIncrement;
import io.atomix.core.map.impl.AtomicCounterMapOperations.IncrementAndGet;
import io.atomix.core.map.impl.AtomicCounterMapOperations.Put;
import io.atomix.core.map.impl.AtomicCounterMapOperations.PutIfAbsent;
import io.atomix.core.map.impl.AtomicCounterMapOperations.Remove;
import io.atomix.core.map.impl.AtomicCounterMapOperations.RemoveValue;
import io.atomix.core.map.impl.AtomicCounterMapOperations.Replace;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.impl.AbstractAsyncPrimitive;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static io.atomix.core.map.impl.AtomicCounterMapOperations.ADD_AND_GET;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.CLEAR;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.DECREMENT_AND_GET;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.GET;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.GET_AND_ADD;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.GET_AND_DECREMENT;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.GET_AND_INCREMENT;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.INCREMENT_AND_GET;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.PUT;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.PUT_IF_ABSENT;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.REMOVE;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.REMOVE_VALUE;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.REPLACE;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.SIZE;

/**
 * {@code AsyncAtomicCounterMap} implementation backed by Atomix.
 */
public class AtomicCounterMapProxy extends AbstractAsyncPrimitive<AsyncAtomicCounterMap<String>> implements AsyncAtomicCounterMap<String> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(AtomicCounterMapOperations.NAMESPACE)
      .build());

  public AtomicCounterMapProxy(PrimitiveProxy proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  protected Serializer serializer() {
    return SERIALIZER;
  }

  @Override
  public CompletableFuture<Long> incrementAndGet(String key) {
    return invokeBy(key, INCREMENT_AND_GET, new IncrementAndGet(key));
  }

  @Override
  public CompletableFuture<Long> decrementAndGet(String key) {
    return invokeBy(key, DECREMENT_AND_GET, new DecrementAndGet(key));
  }

  @Override
  public CompletableFuture<Long> getAndIncrement(String key) {
    return invokeBy(key, GET_AND_INCREMENT, new GetAndIncrement(key));
  }

  @Override
  public CompletableFuture<Long> getAndDecrement(String key) {
    return invokeBy(key, GET_AND_DECREMENT, new GetAndDecrement(key));
  }

  @Override
  public CompletableFuture<Long> addAndGet(String key, long delta) {
    return invokeBy(key, ADD_AND_GET, new AddAndGet(key, delta));
  }

  @Override
  public CompletableFuture<Long> getAndAdd(String key, long delta) {
    return invokeBy(key, GET_AND_ADD, new GetAndAdd(key, delta));
  }

  @Override
  public CompletableFuture<Long> get(String key) {
    return invokeBy(key, GET, new Get(key));
  }

  @Override
  public CompletableFuture<Long> put(String key, long newValue) {
    return invokeBy(key, PUT, new Put(key, newValue));
  }

  @Override
  public CompletableFuture<Long> putIfAbsent(String key, long newValue) {
    return invokeBy(key, PUT_IF_ABSENT, new PutIfAbsent(key, newValue));
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, long expectedOldValue, long newValue) {
    return invokeBy(key, REPLACE, new Replace(key, expectedOldValue, newValue));
  }

  @Override
  public CompletableFuture<Long> remove(String key) {
    return invokeBy(key, REMOVE, new Remove(key));
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, long value) {
    return invokeBy(key, REMOVE_VALUE, new RemoveValue(key, value));
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return size().thenApply(size -> size == 0);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return this.<Integer>invokeAll(SIZE)
        .thenApply(results -> results.reduce(Math::addExact).orElse(0));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return invokeAll(CLEAR).thenApply(v -> null);
  }

  @Override
  public CompletableFuture<AsyncAtomicCounterMap<String>> connect() {
    return super.connect();
  }

  @Override
  public AtomicCounterMap<String> sync(Duration operationTimeout) {
    return new BlockingAtomicCounterMap<>(this, operationTimeout.toMillis());
  }
}