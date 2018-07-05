/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.core.map.impl;

import com.google.common.collect.Maps;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.map.AsyncAtomicTreeMap;
import io.atomix.core.map.AsyncDistributedNavigableMap;
import io.atomix.core.map.AsyncDistributedTreeMap;
import io.atomix.core.map.DistributedTreeMap;
import io.atomix.core.map.MapEventListener;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Delegating asynchronous distributed tree map.
 */
public class DelegatingAsyncDistributedTreeMap<K extends Comparable<K>, V> extends DelegatingAsyncPrimitive implements AsyncDistributedTreeMap<K, V> {
  private final AsyncAtomicTreeMap<K, V> atomicTreeMap;

  public DelegatingAsyncDistributedTreeMap(AsyncAtomicTreeMap<K, V> atomicTreeMap) {
    super(atomicTreeMap);
    this.atomicTreeMap = atomicTreeMap;
  }

  private Map.Entry<K, V> convertEntry(Map.Entry<K, Versioned<V>> entry) {
    return entry == null ? null : Maps.immutableEntry(entry.getKey(), Versioned.valueOrNull(entry.getValue()));
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> lowerEntry(K key) {
    return atomicTreeMap.lowerEntry(key).thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<K> lowerKey(K key) {
    return atomicTreeMap.lowerKey(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> floorEntry(K key) {
    return atomicTreeMap.floorEntry(key).thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<K> floorKey(K key) {
    return atomicTreeMap.floorKey(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> ceilingEntry(K key) {
    return atomicTreeMap.ceilingEntry(key).thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<K> ceilingKey(K key) {
    return atomicTreeMap.ceilingKey(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> higherEntry(K key) {
    return atomicTreeMap.higherEntry(key).thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<K> higherKey(K key) {
    return atomicTreeMap.higherKey(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> firstEntry() {
    return atomicTreeMap.firstEntry().thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> lastEntry() {
    return atomicTreeMap.lastEntry().thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> pollFirstEntry() {
    return atomicTreeMap.pollFirstEntry().thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> pollLastEntry() {
    return atomicTreeMap.pollLastEntry().thenApply(this::convertEntry);
  }

  @Override
  public AsyncDistributedNavigableMap<K, V> descendingMap() {
    return null;
  }

  @Override
  public AsyncDistributedNavigableSet<K> navigableKeySet() {
    return null;
  }

  @Override
  public AsyncDistributedNavigableSet<K> descendingKeySet() {
    return null;
  }

  @Override
  public CompletableFuture<K> firstKey() {
    return null;
  }

  @Override
  public CompletableFuture<K> lastKey() {
    return null;
  }

  @Override
  public CompletableFuture<Integer> size() {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> isEmpty() {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    return null;
  }

  @Override
  public CompletableFuture<V> get(K key) {
    return null;
  }

  @Override
  public CompletableFuture<V> put(K key, V value) {
    return null;
  }

  @Override
  public CompletableFuture<V> remove(K key) {
    return null;
  }

  @Override
  public CompletableFuture<Void> putAll(Map<? extends K, ? extends V> m) {
    return null;
  }

  @Override
  public CompletableFuture<Void> clear() {
    return null;
  }

  @Override
  public AsyncDistributedSet<K> keySet() {
    return null;
  }

  @Override
  public AsyncDistributedCollection<V> values() {
    return null;
  }

  @Override
  public AsyncDistributedSet<Map.Entry<K, V>> entrySet() {
    return null;
  }

  @Override
  public CompletableFuture<V> getOrDefault(K key, V defaultValue) {
    return null;
  }

  @Override
  public CompletableFuture<V> putIfAbsent(K key, V value) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return null;
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return null;
  }

  @Override
  public CompletableFuture<V> replace(K key, V value) {
    return null;
  }

  @Override
  public CompletableFuture<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return null;
  }

  @Override
  public CompletableFuture<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return null;
  }

  @Override
  public CompletableFuture<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return null;
  }

  @Override
  public CompletableFuture<Void> addListener(MapEventListener<K, V> listener, Executor executor) {
    return null;
  }

  @Override
  public CompletableFuture<Void> removeListener(MapEventListener<K, V> listener) {
    return null;
  }

  @Override
  public DistributedTreeMap<K, V> sync(Duration operationTimeout) {
    return null;
  }
}
