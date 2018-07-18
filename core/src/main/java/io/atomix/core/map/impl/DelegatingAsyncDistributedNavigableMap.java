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
import io.atomix.core.map.AsyncAtomicNavigableMap;
import io.atomix.core.map.AsyncDistributedNavigableMap;
import io.atomix.core.map.DistributedNavigableMap;
import io.atomix.core.map.DistributedNavigableMapType;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.primitive.PrimitiveType;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Delegating asynchronous distributed navigable map.
 */
public class DelegatingAsyncDistributedNavigableMap<K extends Comparable<K>, V>
    extends DelegatingAsyncDistributedSortedMap<K, V> implements AsyncDistributedNavigableMap<K, V> {
  private final AsyncAtomicNavigableMap<K, V> atomicMap;

  public DelegatingAsyncDistributedNavigableMap(AsyncAtomicNavigableMap<K, V> atomicMap) {
    super(atomicMap);
    this.atomicMap = atomicMap;
  }

  private Map.Entry<K, V> convertEntry(Map.Entry<K, Versioned<V>> entry) {
    return entry == null ? null : Maps.immutableEntry(entry.getKey(), Versioned.valueOrNull(entry.getValue()));
  }

  @Override
  public PrimitiveType type() {
    return DistributedNavigableMapType.instance();
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> lowerEntry(K key) {
    return atomicMap.lowerEntry(key).thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<K> lowerKey(K key) {
    return atomicMap.lowerKey(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> floorEntry(K key) {
    return atomicMap.floorEntry(key).thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<K> floorKey(K key) {
    return atomicMap.floorKey(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> ceilingEntry(K key) {
    return atomicMap.ceilingEntry(key).thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<K> ceilingKey(K key) {
    return atomicMap.ceilingKey(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> higherEntry(K key) {
    return atomicMap.higherEntry(key).thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<K> higherKey(K key) {
    return atomicMap.higherKey(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> firstEntry() {
    return atomicMap.firstEntry().thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> lastEntry() {
    return atomicMap.lastEntry().thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> pollFirstEntry() {
    return atomicMap.pollFirstEntry().thenApply(this::convertEntry);
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> pollLastEntry() {
    return atomicMap.pollLastEntry().thenApply(this::convertEntry);
  }

  @Override
  public AsyncDistributedNavigableMap<K, V> descendingMap() {
    return new DelegatingAsyncDistributedNavigableMap<>(atomicMap.descendingMap());
  }

  @Override
  public AsyncDistributedNavigableSet<K> navigableKeySet() {
    return atomicMap.navigableKeySet();
  }

  @Override
  public AsyncDistributedNavigableSet<K> descendingKeySet() {
    return atomicMap.descendingKeySet();
  }

  @Override
  public AsyncDistributedNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return new DelegatingAsyncDistributedNavigableMap<>(atomicMap.subMap(fromKey, fromInclusive, toKey, toInclusive));
  }

  @Override
  public AsyncDistributedNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
    return new DelegatingAsyncDistributedNavigableMap<>(atomicMap.headMap(toKey, inclusive));
  }

  @Override
  public AsyncDistributedNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
    return new DelegatingAsyncDistributedNavigableMap<>(atomicMap.tailMap(fromKey, inclusive));
  }

  @Override
  public DistributedNavigableMap<K, V> sync(Duration timeout) {
    return new BlockingDistributedNavigableMap<>(this, timeout.toMillis());
  }
}
