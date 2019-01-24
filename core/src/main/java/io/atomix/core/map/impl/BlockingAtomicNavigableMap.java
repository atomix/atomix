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

import io.atomix.core.map.AsyncAtomicNavigableMap;
import io.atomix.core.map.AtomicNavigableMap;
import io.atomix.core.set.DistributedNavigableSet;
import io.atomix.core.set.impl.BlockingDistributedNavigableSet;
import io.atomix.utils.time.Versioned;

import java.util.Map;

/**
 * Default implementation of {@code AtomicNavigableMap}.
 *
 * @param <K> type of key.
 * @param <V> type of value.
 */
public class BlockingAtomicNavigableMap<K extends Comparable<K>, V> extends BlockingAtomicSortedMap<K, V> implements AtomicNavigableMap<K, V> {

  private final AsyncAtomicNavigableMap<K, V> asyncMap;
  private final long operationTimeoutMillis;

  public BlockingAtomicNavigableMap(AsyncAtomicNavigableMap<K, V> asyncMap, long operationTimeoutMillis) {
    super(asyncMap, operationTimeoutMillis);
    this.asyncMap = asyncMap;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public Map.Entry<K, Versioned<V>> ceilingEntry(K key) {
    return complete(asyncMap.ceilingEntry(key));
  }

  @Override
  public Map.Entry<K, Versioned<V>> floorEntry(K key) {
    return complete(asyncMap.floorEntry(key));
  }

  @Override
  public Map.Entry<K, Versioned<V>> higherEntry(K key) {
    return complete(asyncMap.higherEntry(key));
  }

  @Override
  public Map.Entry<K, Versioned<V>> lowerEntry(K key) {
    return complete(asyncMap.lowerEntry(key));
  }

  @Override
  public Map.Entry<K, Versioned<V>> firstEntry() {
    return complete(asyncMap.firstEntry());
  }

  @Override
  public Map.Entry<K, Versioned<V>> lastEntry() {
    return complete(asyncMap.lastEntry());
  }

  @Override
  public Map.Entry<K, Versioned<V>> pollFirstEntry() {
    return complete(asyncMap.pollFirstEntry());
  }

  @Override
  public Map.Entry<K, Versioned<V>> pollLastEntry() {
    return complete(asyncMap.pollLastEntry());
  }

  @Override
  public K lowerKey(K key) {
    return complete(asyncMap.lowerKey(key));
  }

  @Override
  public K floorKey(K key) {
    return complete(asyncMap.floorKey(key));
  }

  @Override
  public K ceilingKey(K key) {
    return complete(asyncMap.ceilingKey(key));
  }

  @Override
  public K higherKey(K key) {
    return complete(asyncMap.higherKey(key));
  }

  @Override
  public DistributedNavigableSet<K> navigableKeySet() {
    return new BlockingDistributedNavigableSet<>(asyncMap.navigableKeySet(), operationTimeoutMillis);
  }

  @Override
  public AtomicNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return new BlockingAtomicNavigableMap<>(asyncMap.subMap(fromKey, fromInclusive, toKey, toInclusive), operationTimeoutMillis);
  }

  @Override
  public AtomicNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
    return new BlockingAtomicNavigableMap<>(asyncMap.headMap(toKey, inclusive), operationTimeoutMillis);
  }

  @Override
  public AtomicNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
    return new BlockingAtomicNavigableMap<>(asyncMap.tailMap(fromKey, inclusive), operationTimeoutMillis);
  }

  @Override
  public AsyncAtomicNavigableMap<K, V> async() {
    return asyncMap;
  }
}
