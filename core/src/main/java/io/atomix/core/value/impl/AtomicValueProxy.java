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
package io.atomix.core.value.impl;

import com.google.common.collect.Sets;
import io.atomix.core.value.AsyncAtomicValue;
import io.atomix.core.value.AtomicValue;
import io.atomix.core.value.AtomicValueEventListener;
import io.atomix.core.value.impl.AtomicValueOperations.CompareAndSet;
import io.atomix.core.value.impl.AtomicValueOperations.GetAndSet;
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.impl.AbstractAsyncPrimitive;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.atomix.core.value.impl.AtomicValueOperations.ADD_LISTENER;
import static io.atomix.core.value.impl.AtomicValueOperations.COMPARE_AND_SET;
import static io.atomix.core.value.impl.AtomicValueOperations.GET;
import static io.atomix.core.value.impl.AtomicValueOperations.GET_AND_SET;
import static io.atomix.core.value.impl.AtomicValueOperations.REMOVE_LISTENER;
import static io.atomix.core.value.impl.AtomicValueOperations.SET;

/**
 * Atomix counter implementation.
 */
public class AtomicValueProxy extends AbstractAsyncPrimitive<AsyncAtomicValue<byte[]>> implements AsyncAtomicValue<byte[]> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(AtomicValueOperations.NAMESPACE)
      .register(AtomicValueEvents.NAMESPACE)
      .build());

  private final Set<AtomicValueEventListener<byte[]>> eventListeners = Sets.newConcurrentHashSet();

  public AtomicValueProxy(PrimitiveProxy proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  @Override
  public CompletableFuture<byte[]> get() {
    return invoke(getPartitionKey(), GET, SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Void> set(byte[] value) {
    return invoke(getPartitionKey(), SET, SERIALIZER::encode, new AtomicValueOperations.Set(value));
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(byte[] expect, byte[] update) {
    return invoke(getPartitionKey(), COMPARE_AND_SET, SERIALIZER::encode,
        new CompareAndSet(expect, update), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<byte[]> getAndSet(byte[] value) {
    return invoke(getPartitionKey(), GET_AND_SET, SERIALIZER::encode, new GetAndSet(value), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Void> addListener(AtomicValueEventListener<byte[]> listener) {
    if (eventListeners.isEmpty()) {
      return invoke(getPartitionKey(), ADD_LISTENER).thenRun(() -> eventListeners.add(listener));
    } else {
      eventListeners.add(listener);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(AtomicValueEventListener<byte[]> listener) {
    if (eventListeners.remove(listener) && eventListeners.isEmpty()) {
      return invoke(getPartitionKey(), REMOVE_LISTENER).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public AtomicValue<byte[]> sync(Duration operationTimeout) {
    return new BlockingAtomicValue<>(this, operationTimeout.toMillis());
  }
}