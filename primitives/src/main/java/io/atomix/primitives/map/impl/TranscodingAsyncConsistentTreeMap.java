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

package io.atomix.primitives.map.impl;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.atomix.primitives.TransactionId;
import io.atomix.primitives.TransactionLog;
import io.atomix.primitives.map.AsyncConsistentTreeMap;
import io.atomix.primitives.map.MapEvent;
import io.atomix.primitives.map.MapEventListener;
import io.atomix.time.Version;
import io.atomix.time.Versioned;
import io.atomix.utils.concurrent.Futures;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * An {@code AsyncConsistentTreeMap} that maps its operations to operations on
 * a differently typed {@code AsyncConsistentTreeMap} by transcoding operation
 * inputs and outputs.
 *
 * @param <V2> value type of other map
 * @param <V1> value type of this map
 */
public class TranscodingAsyncConsistentTreeMap<K1, V1, K2, V2> implements AsyncConsistentTreeMap<K1, V1> {
  private final AsyncConsistentTreeMap<K2, V2> backingMap;
  private final Function<K1, K2> keyEncoder;
  private final Function<K2, K1> keyDecoder;
  private final Function<V2, V1> valueDecoder;
  private final Function<V1, V2> valueEncoder;
  private final Function<Versioned<V2>, Versioned<V1>>
      versionedValueTransform;
  private final Map<MapEventListener<K1, V1>,
      InternalBackingMapEventListener>
      listeners = Maps.newIdentityHashMap();

  public TranscodingAsyncConsistentTreeMap(
      AsyncConsistentTreeMap<K2, V2> backingMap,
      Function<K1, K2> keyEncoder,
      Function<K2, K1> keyDecoder,
      Function<V1, V2> valueEncoder,
      Function<V2, V1> valueDecoder) {
    this.backingMap = backingMap;
    this.keyEncoder = k -> k == null ? null : keyEncoder.apply(k);
    this.keyDecoder = k -> k == null ? null : keyDecoder.apply(k);
    this.valueEncoder = v -> v == null ? null : valueEncoder.apply(v);
    this.valueDecoder = v -> v == null ? null : valueDecoder.apply(v);
    this.versionedValueTransform = v -> v == null ? null : v.map(valueDecoder);
  }

  @Override
  public CompletableFuture<K1> firstKey() {
    return backingMap.firstKey().thenApply(keyDecoder);
  }

  @Override
  public CompletableFuture<K1> lastKey() {
    return backingMap.lastKey().thenApply(keyDecoder);
  }

  @Override
  public CompletableFuture<Map.Entry<K1, Versioned<V1>>>
  ceilingEntry(K1 key) {
    return backingMap.ceilingEntry(keyEncoder.apply(key))
        .thenApply(entry ->
            Maps.immutableEntry(keyDecoder.apply(entry.getKey()), versionedValueTransform.apply(entry.getValue())));
  }

  @Override
  public CompletableFuture<Map.Entry<K1, Versioned<V1>>>
  floorEntry(K1 key) {
    return backingMap.floorEntry(keyEncoder.apply(key))
        .thenApply(entry -> Maps.immutableEntry(keyDecoder.apply(entry.getKey()), versionedValueTransform.apply(entry.getValue())));
  }

  @Override
  public CompletableFuture<Map.Entry<K1, Versioned<V1>>>
  higherEntry(K1 key) {
    return backingMap.higherEntry(keyEncoder.apply(key))
        .thenApply(entry -> Maps.immutableEntry(keyDecoder.apply(entry.getKey()), versionedValueTransform.apply(entry.getValue())));
  }

  @Override
  public CompletableFuture<Map.Entry<K1, Versioned<V1>>>
  lowerEntry(K1 key) {
    return backingMap.lowerEntry(keyEncoder.apply(key)).thenApply(
        entry -> Maps.immutableEntry(keyDecoder.apply(entry.getKey()), versionedValueTransform.apply(entry.getValue())));
  }

  @Override
  public CompletableFuture<Map.Entry<K1, Versioned<V1>>>
  firstEntry() {
    return backingMap.firstEntry()
        .thenApply(entry -> Maps.immutableEntry(keyDecoder.apply(entry.getKey()), versionedValueTransform.apply(entry.getValue())));
  }

  @Override
  public CompletableFuture<Map.Entry<K1, Versioned<V1>>>
  lastEntry() {
    return backingMap.lastEntry()
        .thenApply(entry -> Maps.immutableEntry(keyDecoder.apply(entry.getKey()), versionedValueTransform.apply(entry.getValue())));
  }

  @Override
  public CompletableFuture<Map.Entry<K1, Versioned<V1>>>
  pollFirstEntry() {
    return backingMap.pollFirstEntry()
        .thenApply(entry -> Maps.immutableEntry(keyDecoder.apply(entry.getKey()), versionedValueTransform.apply(entry.getValue())));
  }

  @Override
  public CompletableFuture<Map.Entry<K1, Versioned<V1>>>
  pollLastEntry() {
    return backingMap.pollLastEntry()
        .thenApply(entry -> Maps.immutableEntry(
            keyDecoder.apply(entry.getKey()),
            versionedValueTransform.apply(entry.getValue())));
  }

  @Override
  public CompletableFuture<K1> lowerKey(K1 key) {
    return backingMap.lowerKey(keyEncoder.apply(key))
        .thenApply(keyDecoder);
  }

  @Override
  public CompletableFuture<K1> floorKey(K1 key) {
    return backingMap.floorKey(keyEncoder.apply(key))
        .thenApply(keyDecoder);
  }

  @Override
  public CompletableFuture<K1> ceilingKey(K1 key) {
    return backingMap.ceilingKey(keyEncoder.apply(key))
        .thenApply(keyDecoder);
  }

  @Override
  public CompletableFuture<K1> higherKey(K1 key) {
    return backingMap.higherKey(keyEncoder.apply(key))
        .thenApply(keyDecoder);
  }

  @Override
  public CompletableFuture<NavigableSet<K1>> navigableKeySet() {
    return backingMap.navigableKeySet()
        .thenApply(set -> set.stream()
        .map(keyDecoder)
        .collect(Collectors.toCollection(TreeSet::new)));
  }

  @Override
  public CompletableFuture<NavigableMap<K1, V1>> subMap(
      K1 upperKey,
      K1 lowerKey,
      boolean inclusiveUpper,
      boolean inclusiveLower) {
    throw new UnsupportedOperationException("This operation is not yet" +
        "supported.");
  }

  @Override
  public String name() {
    return backingMap.name();
  }

  @Override
  public CompletableFuture<Integer> size() {
    return backingMap.size();
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K1 key) {
    return backingMap.containsKey(keyEncoder.apply(key));
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V1 value) {
    return backingMap.containsValue(valueEncoder.apply(value));
  }

  @Override
  public CompletableFuture<Versioned<V1>> get(K1 key) {
    return backingMap.get(keyEncoder.apply(key)).thenApply(versionedValueTransform);
  }

  @Override
  public CompletableFuture<Map<K1, Versioned<V1>>> getAllPresent(Iterable<K1> keys) {
    Set<K2> uniqueKeys = new HashSet<>();
    for (K1 key : keys) {
      uniqueKeys.add(keyEncoder.apply(key));
    }
    return backingMap.getAllPresent(uniqueKeys).thenApply(
            entries -> ImmutableMap.copyOf(entries.entrySet().stream()
                    .collect(Collectors.toMap(o -> keyDecoder.apply(o.getKey()),
                            o -> versionedValueTransform.apply(o.getValue())))));
  }

  @Override
  public CompletableFuture<Versioned<V1>> getOrDefault(K1 key, V1 defaultValue) {
    return backingMap.getOrDefault(keyEncoder.apply(key), valueEncoder.apply(defaultValue)).thenApply(versionedValueTransform);
  }

  @Override
  public CompletableFuture<Versioned<V1>> computeIf(
      K1 key,
      Predicate<? super V1> condition,
      BiFunction<? super K1, ? super V1, ? extends V1> remappingFunction) {
    try {
      return backingMap.computeIf(
          keyEncoder.apply(key),
          v -> condition.test(valueDecoder.apply(v)),
          (k, v) -> valueEncoder.apply(remappingFunction.apply(key, valueDecoder.apply(v))))
          .thenApply(versionedValueTransform);
    } catch (Exception e) {
      return Futures.exceptionalFuture(e);
    }
  }

  @Override
  public CompletableFuture<Versioned<V1>> put(K1 key, V1 value) {
    return backingMap.put(keyEncoder.apply(key), valueEncoder.apply(value))
        .thenApply(versionedValueTransform);
  }

  @Override
  public CompletableFuture<Versioned<V1>> putAndGet(K1 key, V1 value) {
    return backingMap.putAndGet(keyEncoder.apply(key), valueEncoder.apply(value))
        .thenApply(versionedValueTransform);
  }

  @Override
  public CompletableFuture<Versioned<V1>> remove(K1 key) {
    return backingMap.remove(keyEncoder.apply(key)).thenApply(versionedValueTransform);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return backingMap.clear();
  }

  @Override
  public CompletableFuture<Set<K1>> keySet() {
    return backingMap.keySet()
        .thenApply(keys -> keys.stream()
            .map(keyDecoder)
            .collect(Collectors.toSet()));
  }

  @Override
  public CompletableFuture<Collection<Versioned<V1>>> values() {
    return backingMap.values()
        .thenApply(valueSet -> valueSet.stream()
            .map(versionedValueTransform)
            .collect(Collectors.toSet()));
  }

  @Override
  public CompletableFuture<Set<Map.Entry<K1, Versioned<V1>>>> entrySet() {
    return backingMap.entrySet()
        .thenApply(entries -> entries.stream()
            .map(entry -> Maps.immutableEntry(keyDecoder.apply(entry.getKey()), versionedValueTransform.apply(entry.getValue())))
            .collect(Collectors.toSet()));
  }

  @Override
  public CompletableFuture<Versioned<V1>> putIfAbsent(K1 key, V1 value) {
    return backingMap.putIfAbsent(keyEncoder.apply(key), valueEncoder.apply(value))
        .thenApply(versionedValueTransform);
  }

  @Override
  public CompletableFuture<Boolean> remove(K1 key, V1 value) {
    return backingMap.remove(keyEncoder.apply(key), valueEncoder.apply(value));
  }

  @Override
  public CompletableFuture<Boolean> remove(K1 key, long version) {
    return backingMap.remove(keyEncoder.apply(key), version);
  }

  @Override
  public CompletableFuture<Versioned<V1>> replace(K1 key, V1 value) {
    return backingMap.replace(keyEncoder.apply(key), valueEncoder.apply(value))
        .thenApply(versionedValueTransform);
  }

  @Override
  public CompletableFuture<Boolean> replace(K1 key, V1 oldValue,
                                            V1 newValue) {
    return backingMap.replace(keyEncoder.apply(key), valueEncoder.apply(oldValue),
        valueEncoder.apply(newValue));
  }

  @Override
  public CompletableFuture<Boolean> replace(K1 key, long oldVersion,
                                            V1 newValue) {
    return backingMap.replace(keyEncoder.apply(key), oldVersion,
        valueEncoder.apply(newValue));
  }

  @Override
  public CompletableFuture<Void> addListener(
      MapEventListener<K1, V1> listener,
      Executor executor) {
    InternalBackingMapEventListener backingMapEventListener =
        listeners.computeIfAbsent(listener, k -> new InternalBackingMapEventListener(listener));
    return backingMap.addListener(backingMapEventListener, executor);
  }

  @Override
  public CompletableFuture<Void> removeListener(
      MapEventListener<K1, V1> listener) {
    InternalBackingMapEventListener backingMapEventListener = listeners.remove(listener);
    if (backingMapEventListener == null) {
      return CompletableFuture.completedFuture(null);
    } else {
      return backingMap.removeListener(backingMapEventListener);
    }
  }

  @Override
  public CompletableFuture<Version> begin(TransactionId transactionId) {
    throw new UnsupportedOperationException("This operation is not yet supported.");
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<K1, V1>> transactionLog) {
    throw new UnsupportedOperationException("This operation is not yet supported.");
  }

  @Override
  public CompletableFuture<Boolean> prepareAndCommit(TransactionLog<MapUpdate<K1, V1>> transactionLog) {
    throw new UnsupportedOperationException("This operation is not yet supported.");
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    throw new UnsupportedOperationException("This operation is not yet supported.");
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    throw new UnsupportedOperationException("This operation is not yet supported.");
  }

  @Override
  public CompletableFuture<Void> close() {
    return backingMap.close();
  }

  private class InternalBackingMapEventListener implements MapEventListener<K2, V2> {
    private final MapEventListener<K1, V1> listener;

    InternalBackingMapEventListener(MapEventListener<K1, V1> listener) {
      this.listener = listener;
    }

    @Override
    public void event(MapEvent<K2, V2> event) {
      listener.event(new MapEvent<>(
          event.type(),
          event.name(),
          keyDecoder.apply(event.key()),
          event.newValue() != null ?
              event.newValue().map(valueDecoder) : null,
          event.oldValue() != null ?
              event.oldValue().map(valueDecoder) : null));
    }
  }
}
