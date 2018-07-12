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

import io.atomix.core.map.AsyncDistributedNavigableMap;
import io.atomix.core.map.DistributedNavigableMap;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.core.set.impl.AsyncDistributedNavigableJavaSet;
import io.atomix.primitive.protocol.PrimitiveProtocol;

import java.time.Duration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;

/**
 * Java-map wrapped asynchronous distributed navigable map.
 */
public class AsyncDistributedNavigableJavaMap<K extends Comparable<K>, V> extends AsyncDistributedSortedJavaMap<K, V> implements AsyncDistributedNavigableMap<K, V> {
  private final NavigableMap<K, V> map;

  public AsyncDistributedNavigableJavaMap(String name, PrimitiveProtocol protocol, NavigableMap<K, V> map) {
    super(name, protocol, map);
    this.map = map;
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> lowerEntry(K key) {
    return complete(() -> map.lowerEntry(key));
  }

  @Override
  public CompletableFuture<K> lowerKey(K key) {
    return complete(() -> map.lowerKey(key));
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> floorEntry(K key) {
    return complete(() -> map.floorEntry(key));
  }

  @Override
  public CompletableFuture<K> floorKey(K key) {
    return complete(() -> map.floorKey(key));
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> ceilingEntry(K key) {
    return complete(() -> map.ceilingEntry(key));
  }

  @Override
  public CompletableFuture<K> ceilingKey(K key) {
    return complete(() -> map.ceilingKey(key));
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> higherEntry(K key) {
    return complete(() -> map.higherEntry(key));
  }

  @Override
  public CompletableFuture<K> higherKey(K key) {
    return complete(() -> map.higherKey(key));
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> firstEntry() {
    return complete(() -> map.firstEntry());
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> lastEntry() {
    return complete(() -> map.lastEntry());
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> pollFirstEntry() {
    return complete(() -> map.pollFirstEntry());
  }

  @Override
  public CompletableFuture<Map.Entry<K, V>> pollLastEntry() {
    return complete(() -> map.pollLastEntry());
  }

  @Override
  public AsyncDistributedNavigableMap<K, V> descendingMap() {
    return new AsyncDistributedNavigableJavaMap<>(name(), protocol(), map.descendingMap());
  }

  @Override
  public AsyncDistributedNavigableSet<K> navigableKeySet() {
    return new AsyncDistributedNavigableJavaSet<>(name(), protocol(), map.navigableKeySet());
  }

  @Override
  public AsyncDistributedNavigableSet<K> descendingKeySet() {
    return new AsyncDistributedNavigableJavaSet<>(name(), protocol(), map.descendingKeySet());
  }

  @Override
  public AsyncDistributedNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return new AsyncDistributedNavigableJavaMap<>(name(), protocol(), map.subMap(fromKey, fromInclusive, toKey, toInclusive));
  }

  @Override
  public AsyncDistributedNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
    return new AsyncDistributedNavigableJavaMap<>(name(), protocol(), map.headMap(toKey, inclusive));
  }

  @Override
  public AsyncDistributedNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
    return new AsyncDistributedNavigableJavaMap<>(name(), protocol(), map.tailMap(fromKey, inclusive));
  }

  @Override
  public DistributedNavigableMap<K, V> sync(Duration operationTimeout) {
    return new BlockingDistributedNavigableMap<>(this, operationTimeout.toMillis());
  }
}
