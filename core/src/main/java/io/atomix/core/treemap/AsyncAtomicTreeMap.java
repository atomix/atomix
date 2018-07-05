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

package io.atomix.core.treemap;

import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.CompletableFuture;

/**
 * API for a distributed tree map implementation.
 */
public interface AsyncAtomicTreeMap<K extends Comparable<K>, V> extends AsyncAtomicMap<K, V> {

  /**
   * Return the lowest key in the map.
   *
   * @return the key or null if none exist
   */
  CompletableFuture<K> firstKey();

  /**
   * Return the highest key in the map.
   *
   * @return the key or null if none exist
   */
  CompletableFuture<K> lastKey();

  /**
   * Returns the entry associated with the least key greater than or equal to
   * the key.
   *
   * @param key the key
   * @return the entry or null if no suitable key exists
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> ceilingEntry(K key);

  /**
   * Returns the entry associated with the greatest key less than or equal
   * to key.
   *
   * @param key the key
   * @return the entry or null if no suitable key exists
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> floorEntry(K key);

  /**
   * Returns the entry associated with the least key greater than key.
   *
   * @param key the key
   * @return the entry or null if no suitable key exists
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> higherEntry(K key);

  /**
   * Returns the entry associated with the largest key less than key.
   *
   * @param key the key
   * @return the entry or null if no suitable key exists
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> lowerEntry(K key);

  /**
   * Return the entry associated with the lowest key in the map.
   *
   * @return the entry or null if none exist
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> firstEntry();

  /**
   * Return the entry associated with the highest key in the map.
   *
   * @return the entry or null if none exist
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> lastEntry();

  /**
   * Return the entry associated with the greatest key less than key.
   *
   * @param key the key
   * @return the entry or null if no suitable key exists
   */
  CompletableFuture<K> lowerKey(K key);

  /**
   * Return the highest key less than or equal to key.
   *
   * @param key the key
   * @return the entry or null if no suitable key exists
   */
  CompletableFuture<K> floorKey(K key);

  /**
   * Return the lowest key greater than or equal to key.
   *
   * @param key the key
   * @return the entry or null if no suitable key exists
   */
  CompletableFuture<K> ceilingKey(K key);

  /**
   * Return the lowest key greater than key.
   *
   * @param key the key
   * @return the entry or null if no suitable key exists
   */
  CompletableFuture<K> higherKey(K key);

  /**
   * Returns a navigable set of the keys in this map.
   *
   * @return a navigable key set (this may be empty)
   */
  CompletableFuture<NavigableSet<K>> navigableKeySet();

  /**
   * Returns a navigable map containing the entries from the original map
   * which are larger than (or if specified equal to) {@code lowerKey} AND
   * less than (or if specified equal to) {@code upperKey}.
   *
   * @param upperKey       the upper bound for the keys in this map
   * @param lowerKey       the lower bound for the keys in this map
   * @param inclusiveUpper whether keys equal to the upperKey should be
   *                       included
   * @param inclusiveLower whether keys equal to the lowerKey should be
   *                       included
   * @return a navigable map containing entries in the specified range (this
   * may be empty)
   */
  CompletableFuture<NavigableMap<K, V>> subMap(
      K upperKey,
      K lowerKey,
      boolean inclusiveUpper,
      boolean inclusiveLower);

  @Override
  default AtomicTreeMap<K, V> sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  AtomicTreeMap<K, V> sync(Duration operationTimeout);
}
