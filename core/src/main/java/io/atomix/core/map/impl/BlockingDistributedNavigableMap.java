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
package io.atomix.core.map.impl;

import io.atomix.core.map.AsyncDistributedNavigableMap;
import io.atomix.core.map.DistributedNavigableMap;
import io.atomix.core.set.DistributedNavigableSet;
import io.atomix.core.set.impl.BlockingDistributedNavigableSet;

/**
 * Default implementation of {@code DistributedNavigableMap}.
 *
 * @param <K> type of key.
 * @param <V> type of value.
 */
public class BlockingDistributedNavigableMap<K extends Comparable<K>, V> extends BlockingDistributedSortedMap<K, V> implements DistributedNavigableMap<K, V> {

  private final long operationTimeoutMillis;
  private final AsyncDistributedNavigableMap<K, V> asyncMap;

  public BlockingDistributedNavigableMap(AsyncDistributedNavigableMap<K, V> asyncMap, long operationTimeoutMillis) {
    super(asyncMap, operationTimeoutMillis);
    this.asyncMap = asyncMap;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public Entry<K, V> lowerEntry(K key) {
    return complete(asyncMap.lowerEntry(key));
  }

  @Override
  public K lowerKey(K key) {
    return complete(asyncMap.lowerKey(key));
  }

  @Override
  public Entry<K, V> floorEntry(K key) {
    return complete(asyncMap.floorEntry(key));
  }

  @Override
  public K floorKey(K key) {
    return complete(asyncMap.floorKey(key));
  }

  @Override
  public Entry<K, V> ceilingEntry(K key) {
    return complete(asyncMap.ceilingEntry(key));
  }

  @Override
  public K ceilingKey(K key) {
    return complete(asyncMap.ceilingKey(key));
  }

  @Override
  public Entry<K, V> higherEntry(K key) {
    return complete(asyncMap.higherEntry(key));
  }

  @Override
  public K higherKey(K key) {
    return complete(asyncMap.higherKey(key));
  }

  @Override
  public Entry<K, V> firstEntry() {
    return complete(asyncMap.firstEntry());
  }

  @Override
  public Entry<K, V> lastEntry() {
    return complete(asyncMap.lastEntry());
  }

  @Override
  public Entry<K, V> pollFirstEntry() {
    return complete(asyncMap.pollFirstEntry());
  }

  @Override
  public Entry<K, V> pollLastEntry() {
    return complete(asyncMap.pollLastEntry());
  }

  @Override
  public DistributedNavigableMap<K, V> descendingMap() {
    return new BlockingDistributedNavigableMap<>(asyncMap.descendingMap(), operationTimeoutMillis);
  }

  @Override
  public DistributedNavigableSet<K> navigableKeySet() {
    return new BlockingDistributedNavigableSet<>(asyncMap.navigableKeySet(), operationTimeoutMillis);
  }

  @Override
  public DistributedNavigableSet<K> descendingKeySet() {
    return new BlockingDistributedNavigableSet<>(asyncMap.descendingKeySet(), operationTimeoutMillis);
  }

  @Override
  public DistributedNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return new BlockingDistributedNavigableMap<>(asyncMap.subMap(fromKey, fromInclusive, toKey, toInclusive), operationTimeoutMillis);
  }

  @Override
  public DistributedNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
    return new BlockingDistributedNavigableMap<>(asyncMap.headMap(toKey, inclusive), operationTimeoutMillis);
  }

  @Override
  public DistributedNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
    return new BlockingDistributedNavigableMap<>(asyncMap.tailMap(fromKey, inclusive), operationTimeoutMillis);
  }

  @Override
  public AsyncDistributedNavigableMap<K, V> async() {
    return asyncMap;
  }
}
