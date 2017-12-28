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

package io.atomix.core.multimap.impl;

import com.google.common.collect.Multiset;

import io.atomix.primitive.impl.AbstractAsyncPrimitive;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.core.multimap.AsyncConsistentMultimap;
import io.atomix.core.multimap.ConsistentMultimap;
import io.atomix.core.multimap.MultimapEvent;
import io.atomix.core.multimap.MultimapEventListener;
import io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.ContainsEntry;
import io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.ContainsKey;
import io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.ContainsValue;
import io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.Get;
import io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.MultiRemove;
import io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.Put;
import io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.RemoveAll;
import io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.Replace;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import static io.atomix.core.multimap.impl.ConsistentSetMultimapEvents.CHANGE;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.ADD_LISTENER;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.CLEAR;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.CONTAINS_ENTRY;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.CONTAINS_KEY;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.CONTAINS_VALUE;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.ENTRIES;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.GET;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.IS_EMPTY;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.KEYS;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.KEY_SET;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.PUT;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.REMOVE;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.REMOVE_ALL;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.REMOVE_LISTENER;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.REPLACE;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.SIZE;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.VALUES;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

/**
 * Set based implementation of the {@link AsyncConsistentMultimap}.
 * <p>
 * Note: this implementation does not allow null entries or duplicate entries.
 */
public class ConsistentSetMultimapProxy
    extends AbstractAsyncPrimitive
    implements AsyncConsistentMultimap<String, byte[]> {

  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(ConsistentSetMultimapOperations.NAMESPACE)
      .register(ConsistentSetMultimapEvents.NAMESPACE)
      .build());

  private final Map<MultimapEventListener<String, byte[]>, Executor> mapEventListeners = new ConcurrentHashMap<>();

  public ConsistentSetMultimapProxy(PrimitiveProxy proxy) {
    super(proxy);
    proxy.addEventListener(CHANGE, SERIALIZER::decode, this::handleEvent);
    proxy.addStateChangeListener(state -> {
      if (state == PrimitiveProxy.State.CONNECTED && isListening()) {
        proxy.invoke(ADD_LISTENER);
      }
    });
  }

  private void handleEvent(List<MultimapEvent<String, byte[]>> events) {
    events.forEach(event ->
        mapEventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event))));
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
  public CompletableFuture<Boolean> containsKey(String key) {
    return proxy.invoke(CONTAINS_KEY, SERIALIZER::encode, new ContainsKey(key), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return proxy.invoke(CONTAINS_VALUE, SERIALIZER::encode, new ContainsValue(value), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> containsEntry(String key, byte[] value) {
    return proxy.invoke(CONTAINS_ENTRY, SERIALIZER::encode, new ContainsEntry(key, value), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> put(String key, byte[] value) {
    return proxy.invoke(
        PUT,
        SERIALIZER::encode,
        new Put(key, Collections.singletonList(value), null),
        SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, byte[] value) {
    return proxy.invoke(REMOVE, SERIALIZER::encode, new MultiRemove(key,
            Collections.singletonList(value), null), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> removeAll(String key, Collection<? extends byte[]> values) {
    return proxy.invoke(
        REMOVE,
        SERIALIZER::encode,
        new MultiRemove(key, (Collection<byte[]>) values, null),
        SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends byte[]>>> removeAll(String key) {
    return proxy.invoke(REMOVE_ALL, SERIALIZER::encode, new RemoveAll(key, null), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> putAll(
      String key, Collection<? extends byte[]> values) {
    return proxy.invoke(PUT, SERIALIZER::encode, new Put(key, values, null), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends byte[]>>> replaceValues(
      String key, Collection<byte[]> values) {
    return proxy.invoke(
        REPLACE,
        SERIALIZER::encode,
        new Replace(key, values, null),
        SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return proxy.invoke(CLEAR);
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends byte[]>>> get(String key) {
    return proxy.invoke(GET, SERIALIZER::encode, new Get(key), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Set<String>> keySet() {
    return proxy.invoke(KEY_SET, SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Multiset<String>> keys() {
    return proxy.invoke(KEYS, SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Multiset<byte[]>> values() {
    return proxy.invoke(VALUES, SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Collection<Map.Entry<String, byte[]>>> entries() {
    return proxy.invoke(ENTRIES, SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Void> addListener(MultimapEventListener<String, byte[]> listener, Executor executor) {
    if (mapEventListeners.isEmpty()) {
      return proxy.invoke(ADD_LISTENER).thenRun(() -> mapEventListeners.put(listener, executor));
    } else {
      mapEventListeners.put(listener, executor);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(MultimapEventListener<String, byte[]> listener) {
    if (mapEventListeners.remove(listener) != null && mapEventListeners.isEmpty()) {
      return proxy.invoke(REMOVE_LISTENER).thenApply(v -> null);
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Map<String, Collection<byte[]>>> asMap() {
    throw new UnsupportedOperationException("Expensive operation.");
  }

  private boolean isListening() {
    return !mapEventListeners.isEmpty();
  }

  @Override
  public ConsistentMultimap<String, byte[]> sync(Duration operationTimeout) {
    return new BlockingConsistentMultimap<>(this, operationTimeout.toMillis());
  }
}