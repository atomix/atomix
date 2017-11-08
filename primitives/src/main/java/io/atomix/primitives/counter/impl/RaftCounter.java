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
package io.atomix.primitives.counter.impl;

import io.atomix.primitives.counter.AsyncAtomicCounter;
import io.atomix.primitives.counter.impl.RaftCounterOperations.AddAndGet;
import io.atomix.primitives.counter.impl.RaftCounterOperations.CompareAndSet;
import io.atomix.primitives.counter.impl.RaftCounterOperations.GetAndAdd;
import io.atomix.primitives.counter.impl.RaftCounterOperations.Set;
import io.atomix.primitives.impl.AbstractRaftPrimitive;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;

import java.util.concurrent.CompletableFuture;

import static io.atomix.primitives.counter.impl.RaftCounterOperations.ADD_AND_GET;
import static io.atomix.primitives.counter.impl.RaftCounterOperations.COMPARE_AND_SET;
import static io.atomix.primitives.counter.impl.RaftCounterOperations.GET;
import static io.atomix.primitives.counter.impl.RaftCounterOperations.GET_AND_ADD;
import static io.atomix.primitives.counter.impl.RaftCounterOperations.GET_AND_INCREMENT;
import static io.atomix.primitives.counter.impl.RaftCounterOperations.INCREMENT_AND_GET;
import static io.atomix.primitives.counter.impl.RaftCounterOperations.SET;

/**
 * Atomix counter implementation.
 */
public class RaftCounter extends AbstractRaftPrimitive implements AsyncAtomicCounter {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
      .register(KryoNamespaces.BASIC)
      .register(RaftCounterOperations.NAMESPACE)
      .build());

  public RaftCounter(RaftProxy proxy) {
    super(proxy);
  }

  private long nullOrZero(Long value) {
    return value != null ? value : 0;
  }

  @Override
  public CompletableFuture<Long> get() {
    return proxy.<Long>invoke(GET, SERIALIZER::decode).thenApply(this::nullOrZero);
  }

  @Override
  public CompletableFuture<Void> set(long value) {
    return proxy.invoke(SET, SERIALIZER::encode, new Set(value));
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(long expectedValue, long updateValue) {
    return proxy.invoke(COMPARE_AND_SET, SERIALIZER::encode,
        new CompareAndSet(expectedValue, updateValue), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Long> addAndGet(long delta) {
    return proxy.invoke(ADD_AND_GET, SERIALIZER::encode, new AddAndGet(delta), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Long> getAndAdd(long delta) {
    return proxy.invoke(GET_AND_ADD, SERIALIZER::encode, new GetAndAdd(delta), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Long> incrementAndGet() {
    return proxy.invoke(INCREMENT_AND_GET, SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Long> getAndIncrement() {
    return proxy.invoke(GET_AND_INCREMENT, SERIALIZER::decode);
  }
}