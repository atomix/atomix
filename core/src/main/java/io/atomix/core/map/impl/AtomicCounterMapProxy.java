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
import io.atomix.primitive.impl.AbstractAsyncPrimitive;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import static io.atomix.core.map.impl.AtomicCounterMapOperations.ADD_AND_GET;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.CLEAR;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.DECREMENT_AND_GET;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.GET;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.GET_AND_ADD;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.GET_AND_DECREMENT;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.GET_AND_INCREMENT;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.INCREMENT_AND_GET;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.IS_EMPTY;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.PUT;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.PUT_IF_ABSENT;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.REMOVE;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.REMOVE_VALUE;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.REPLACE;
import static io.atomix.core.map.impl.AtomicCounterMapOperations.SIZE;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * {@code AsyncAtomicCounterMap} implementation backed by Atomix.
 */
public class AtomicCounterMapProxy extends AbstractAsyncPrimitive implements AsyncAtomicCounterMap<String> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(AtomicCounterMapOperations.NAMESPACE)
      .build());

  public AtomicCounterMapProxy(PrimitiveProxy proxy) {
    super(proxy);
  }

  @Override
  public CompletableFuture<Long> incrementAndGet(String key) {
    return proxy.invoke(INCREMENT_AND_GET, SERIALIZER::encode, new IncrementAndGet(key), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Long> decrementAndGet(String key) {
    return proxy.invoke(DECREMENT_AND_GET, SERIALIZER::encode, new DecrementAndGet(key), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Long> getAndIncrement(String key) {
    return proxy.invoke(GET_AND_INCREMENT, SERIALIZER::encode, new GetAndIncrement(key), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Long> getAndDecrement(String key) {
    return proxy.invoke(GET_AND_DECREMENT, SERIALIZER::encode, new GetAndDecrement(key), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Long> addAndGet(String key, long delta) {
    return proxy.invoke(ADD_AND_GET, SERIALIZER::encode, new AddAndGet(key, delta), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Long> getAndAdd(String key, long delta) {
    return proxy.invoke(GET_AND_ADD, SERIALIZER::encode, new GetAndAdd(key, delta), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Long> get(String key) {
    return proxy.invoke(GET, SERIALIZER::encode, new Get(key), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Long> put(String key, long newValue) {
    return proxy.invoke(PUT, SERIALIZER::encode, new Put(key, newValue), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Long> putIfAbsent(String key, long newValue) {
    return proxy.invoke(PUT_IF_ABSENT, SERIALIZER::encode, new PutIfAbsent(key, newValue), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, long expectedOldValue, long newValue) {
    return proxy.invoke(
        REPLACE,
        SERIALIZER::encode,
        new Replace(key, expectedOldValue, newValue),
        SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Long> remove(String key) {
    return proxy.invoke(REMOVE, SERIALIZER::encode, new Remove(key), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, long value) {
    return proxy.invoke(REMOVE_VALUE, SERIALIZER::encode, new RemoveValue(key, value), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return proxy.invoke(SIZE, SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return proxy.invoke(IS_EMPTY, SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return proxy.invoke(CLEAR);
  }

  @Override
  public AtomicCounterMap<String> sync(Duration operationTimeout) {
    return new BlockingAtomicCounterMap<>(this, operationTimeout.toMillis());
  }
}