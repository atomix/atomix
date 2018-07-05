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

package io.atomix.core.map;

import io.atomix.utils.time.Versioned;

import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;

/**
 * Tree map interface counterpart to {@link AsyncAtomicTreeMap}.
 */
public interface AtomicTreeMap<K extends Comparable<K>, V> extends AtomicMap<K, V> {

  /**
   * Returns the lowest key in the map.
   *
   * @return the key or null if none exist
   */
  K firstKey();

  /**
   * Returns the highest key in the map.
   *
   * @return the key or null if none exist
   */
  K lastKey();

  /**
   * Returns the entry associated with the least key greater than or equal to the key.
   *
   * @param key the key
   * @return the entry or null
   */
  Map.Entry<K, Versioned<V>> ceilingEntry(K key);

  /**
   * Returns the entry associated with the greatest key less than or equal to key.
   *
   * @param key the key
   * @return the entry or null
   */
  Map.Entry<K, Versioned<V>> floorEntry(K key);

  /**
   * Returns the entry associated with the lest key greater than key.
   *
   * @param key the key
   * @return the entry or null
   */
  Map.Entry<K, Versioned<V>> higherEntry(K key);

  /**
   * Returns the entry associated with the largest key less than key.
   *
   * @param key the key
   * @return the entry or null
   */
  Map.Entry<K, Versioned<V>> lowerEntry(K key);

  /**
   * Returns the entry associated with the lowest key in the map.
   *
   * @return the entry or null
   */
  Map.Entry<K, Versioned<V>> firstEntry();

  /**
   * Returns the entry associated with the highest key in the map.
   *
   * @return the entry or null
   */
  Map.Entry<K, Versioned<V>> lastEntry();

  /**
   * Returns the entry associated with the greatest key less than key.
   *
   * @param key the key
   * @return the entry or null
   */
  K lowerKey(K key);

  /**
   * Returns the entry associated with the highest key less than or equal to key.
   *
   * @param key the key
   * @return the entry or null
   */
  K floorKey(K key);

  /**
   * Returns the lowest key greater than or equal to key.
   *
   * @param key the key
   * @return the key or null
   */
  K ceilingKey(K key);

  /**
   * Returns the lowest key greater than key.
   *
   * @param key the key
   * @return the key or null
   */
  K higherKey(K key);

  /**
   * Returns a navigable set of the keys in this map.
   *
   * @return a navigable key set
   */
  NavigableSet<K> navigableKeySet();

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
  NavigableMap<K, V> subMap(
      K upperKey,
      K lowerKey,
      boolean inclusiveUpper,
      boolean inclusiveLower);

  @Override
  AsyncAtomicTreeMap<K, V> async();
}
