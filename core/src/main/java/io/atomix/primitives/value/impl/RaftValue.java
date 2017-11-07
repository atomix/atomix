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
package io.atomix.primitives.value.impl;

import com.google.common.collect.Sets;
import io.atomix.primitives.impl.AbstractRaftPrimitive;
import io.atomix.primitives.value.AsyncAtomicValue;
import io.atomix.primitives.value.AtomicValueEventListener;
import io.atomix.primitives.value.impl.RaftValueOperations.CompareAndSet;
import io.atomix.primitives.value.impl.RaftValueOperations.GetAndSet;
import io.atomix.protocols.raft.proxy.RaftProxy;
import io.atomix.serializer.Serializer;
import io.atomix.serializer.kryo.KryoNamespace;
import io.atomix.serializer.kryo.KryoNamespaces;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.atomix.primitives.value.impl.RaftValueOperations.ADD_LISTENER;
import static io.atomix.primitives.value.impl.RaftValueOperations.COMPARE_AND_SET;
import static io.atomix.primitives.value.impl.RaftValueOperations.GET;
import static io.atomix.primitives.value.impl.RaftValueOperations.GET_AND_SET;
import static io.atomix.primitives.value.impl.RaftValueOperations.REMOVE_LISTENER;
import static io.atomix.primitives.value.impl.RaftValueOperations.SET;

/**
 * Atomix counter implementation.
 */
public class RaftValue extends AbstractRaftPrimitive implements AsyncAtomicValue<byte[]> {
  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.newBuilder()
      .register(KryoNamespaces.BASIC)
      .register(RaftValueOperations.NAMESPACE)
      .register(RaftValueEvents.NAMESPACE)
      .build());

  private final Set<AtomicValueEventListener<byte[]>> eventListeners = Sets.newConcurrentHashSet();

  public RaftValue(RaftProxy proxy) {
    super(proxy);
  }

  @Override
  public CompletableFuture<byte[]> get() {
    return proxy.invoke(GET, SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Void> set(byte[] value) {
    return proxy.invoke(SET, SERIALIZER::encode, new RaftValueOperations.Set(value));
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(byte[] expect, byte[] update) {
    return proxy.invoke(COMPARE_AND_SET, SERIALIZER::encode,
        new CompareAndSet(expect, update), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<byte[]> getAndSet(byte[] value) {
    return proxy.invoke(GET_AND_SET, SERIALIZER::encode, new GetAndSet(value), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Void> addListener(AtomicValueEventListener<byte[]> listener) {
    if (eventListeners.isEmpty()) {
      return proxy.invoke(ADD_LISTENER).thenRun(() -> eventListeners.add(listener));
    } else {
      eventListeners.add(listener);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(AtomicValueEventListener<byte[]> listener) {
    if (eventListeners.remove(listener) && eventListeners.isEmpty()) {
      return proxy.invoke(REMOVE_LISTENER).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }
}