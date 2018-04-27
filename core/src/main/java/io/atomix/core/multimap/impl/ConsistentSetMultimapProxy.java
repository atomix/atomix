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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
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
import io.atomix.primitive.PrimitiveRegistry;
import io.atomix.primitive.impl.AbstractAsyncPrimitive;
import io.atomix.primitive.proxy.PartitionProxy;
import io.atomix.primitive.proxy.PrimitiveProxy;
import io.atomix.utils.serializer.KryoNamespace;
import io.atomix.utils.serializer.KryoNamespaces;
import io.atomix.utils.serializer.Serializer;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Predicate;

import static io.atomix.core.multimap.impl.ConsistentSetMultimapEvents.CHANGE;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.ADD_LISTENER;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.CLEAR;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.CONTAINS_ENTRY;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.CONTAINS_KEY;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.CONTAINS_VALUE;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.ENTRIES;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.GET;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.KEYS;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.KEY_SET;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.PUT;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.REMOVE;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.REMOVE_ALL;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.REMOVE_LISTENER;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.REPLACE;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.SIZE;
import static io.atomix.core.multimap.impl.ConsistentSetMultimapOperations.VALUES;

/**
 * Set based implementation of the {@link AsyncConsistentMultimap}.
 * <p>
 * Note: this implementation does not allow null entries or duplicate entries.
 */
public class ConsistentSetMultimapProxy
    extends AbstractAsyncPrimitive<AsyncConsistentMultimap<String, byte[]>>
    implements AsyncConsistentMultimap<String, byte[]> {

  private static final Serializer SERIALIZER = Serializer.using(KryoNamespace.builder()
      .register(KryoNamespaces.BASIC)
      .register(ConsistentSetMultimapOperations.NAMESPACE)
      .register(ConsistentSetMultimapEvents.NAMESPACE)
      .build());

  private final Map<MultimapEventListener<String, byte[]>, Executor> mapEventListeners = new ConcurrentHashMap<>();

  public ConsistentSetMultimapProxy(PrimitiveProxy proxy, PrimitiveRegistry registry) {
    super(proxy, registry);
  }

  private void handleEvent(List<MultimapEvent<String, byte[]>> events) {
    events.forEach(event ->
        mapEventListeners.forEach((listener, executor) -> executor.execute(() -> listener.event(event))));
  }

  @Override
  public CompletableFuture<Integer> size() {
    return this.<Integer>invokes(SIZE, SERIALIZER::decode)
        .thenApply(results -> results.reduce(Math::addExact).orElse(0));
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return size().thenApply(size -> size == 0);
  }

  @Override
  public CompletableFuture<Boolean> containsKey(String key) {
    return invoke(key, CONTAINS_KEY, SERIALIZER::encode, new ContainsKey(key), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> containsValue(byte[] value) {
    return this.<ContainsValue, Boolean>invokes(
        CONTAINS_VALUE,
        SERIALIZER::encode,
        new ContainsValue(value),
        SERIALIZER::decode)
        .thenApply(results -> results.filter(Predicate.isEqual(true)).findFirst().orElse(false));
  }

  @Override
  public CompletableFuture<Boolean> containsEntry(String key, byte[] value) {
    return this.<ContainsEntry, Boolean>invokes(
        CONTAINS_ENTRY,
        SERIALIZER::encode,
        new ContainsEntry(key, value),
        SERIALIZER::decode)
        .thenApply(results -> results.filter(Predicate.isEqual(true)).findFirst().orElse(false));
  }

  @Override
  public CompletableFuture<Boolean> put(String key, byte[] value) {
    return invoke(
        key,
        PUT,
        SERIALIZER::encode,
        new Put(key, Collections.singletonList(value), null),
        SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, byte[] value) {
    return invoke(key, REMOVE, SERIALIZER::encode, new MultiRemove(key,
        Collections.singletonList(value), null), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> removeAll(String key, Collection<? extends byte[]> values) {
    return invoke(
        key,
        REMOVE,
        SERIALIZER::encode,
        new MultiRemove(key, (Collection<byte[]>) values, null),
        SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends byte[]>>> removeAll(String key) {
    return invoke(
        key,
        REMOVE_ALL,
        SERIALIZER::encode,
        new RemoveAll(key, null),
        SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Boolean> putAll(String key, Collection<? extends byte[]> values) {
    return invoke(
        key,
        PUT,
        SERIALIZER::encode,
        new Put(key, values, null),
        SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends byte[]>>> replaceValues(
      String key, Collection<byte[]> values) {
    return invoke(
        key,
        REPLACE,
        SERIALIZER::encode,
        new Replace(key, values, null),
        SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return invokes(CLEAR);
  }

  @Override
  public CompletableFuture<Versioned<Collection<? extends byte[]>>> get(String key) {
    return invoke(key, GET, SERIALIZER::encode, new Get(key), SERIALIZER::decode);
  }

  @Override
  public CompletableFuture<Set<String>> keySet() {
    return this.<Set<String>>invokes(KEY_SET, SERIALIZER::decode)
        .thenApply(results -> results.reduce((s1, s2) -> ImmutableSet.<String>builder().addAll(s1).addAll(s2).build()).orElse(ImmutableSet.of()));
  }

  @Override
  public CompletableFuture<Multiset<String>> keys() {
    return this.<Multiset<String>>invokes(KEYS, SERIALIZER::decode)
        .thenApply(results -> results.reduce(Multisets::sum).orElse(HashMultiset.create()));
  }

  @Override
  public CompletableFuture<Multiset<byte[]>> values() {
    return this.<Multiset<byte[]>>invokes(VALUES, SERIALIZER::decode)
        .thenApply(results -> results.reduce(Multisets::sum).orElse(HashMultiset.create()));
  }

  @Override
  public CompletableFuture<Collection<Map.Entry<String, byte[]>>> entries() {
    return this.<Collection<Map.Entry<String, byte[]>>>invokes(ENTRIES, SERIALIZER::decode)
        .thenApply(results -> results.reduce((s1, s2) -> ImmutableList.copyOf(Iterables.concat(s1, s2))).orElse(ImmutableList.of()));
  }

  @Override
  public CompletableFuture<Void> addListener(MultimapEventListener<String, byte[]> listener, Executor executor) {
    if (mapEventListeners.isEmpty()) {
      return invokes(ADD_LISTENER);
    } else {
      mapEventListeners.put(listener, executor);
      return CompletableFuture.completedFuture(null);
    }
  }

  @Override
  public CompletableFuture<Void> removeListener(MultimapEventListener<String, byte[]> listener) {
    if (mapEventListeners.remove(listener) != null && mapEventListeners.isEmpty()) {
      return invokes(REMOVE_LISTENER);
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
  public CompletableFuture<AsyncConsistentMultimap<String, byte[]>> connect() {
    return super.connect()
        .thenRun(() -> getPartitions().forEach(partition -> {
          partition.addEventListener(CHANGE, SERIALIZER::decode, this::handleEvent);
          partition.addStateChangeListener(state -> {
            if (state == PartitionProxy.State.CONNECTED && isListening()) {
              partition.invoke(ADD_LISTENER);
            }
          });
        }))
        .thenApply(v -> this);
  }

  @Override
  public ConsistentMultimap<String, byte[]> sync(Duration operationTimeout) {
    return new BlockingConsistentMultimap<>(this, operationTimeout.toMillis());
  }
}