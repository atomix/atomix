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

import io.atomix.core.set.DistributedNavigableSet;
import io.atomix.utils.time.Versioned;

import java.util.Map;

/**
 * Atomic navigable map.
 */
public interface AtomicNavigableMap<K extends Comparable<K>, V> extends AtomicSortedMap<K, V> {

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
   * Returns the entry associated with the lowest key in the map.
   *
   * @return the entry or null
   */
  Map.Entry<K, Versioned<V>> pollFirstEntry();

  /**
   * Returns the entry associated with the highest key in the map.
   *
   * @return the entry or null
   */
  Map.Entry<K, Versioned<V>> pollLastEntry();

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
  DistributedNavigableSet<K> navigableKeySet();

  @Override
  default AtomicSortedMap<K, V> subMap(K fromKey, K toKey) {
    return subMap(fromKey, true, toKey, false);
  }

  /**
   * Returns a view of the portion of this map whose keys range from
   * {@code fromKey} to {@code toKey}.  If {@code fromKey} and
   * {@code toKey} are equal, the returned map is empty unless
   * {@code fromInclusive} and {@code toInclusive} are both true.  The
   * returned map is backed by this map, so changes in the returned map are
   * reflected in this map, and vice-versa.  The returned map supports all
   * optional map operations that this map supports.
   *
   * <p>The returned map will throw an {@code IllegalArgumentException}
   * on an attempt to insert a key outside of its range, or to construct a
   * submap either of whose endpoints lie outside its range.
   *
   * @param fromKey low endpoint of the keys in the returned map
   * @param fromInclusive {@code true} if the low endpoint
   *        is to be included in the returned view
   * @param toKey high endpoint of the keys in the returned map
   * @param toInclusive {@code true} if the high endpoint
   *        is to be included in the returned view
   * @return a view of the portion of this map whose keys range from
   *         {@code fromKey} to {@code toKey}
   * @throws ClassCastException if {@code fromKey} and {@code toKey}
   *         cannot be compared to one another using this map's comparator
   *         (or, if the map has no comparator, using natural ordering).
   *         Implementations may, but are not required to, throw this
   *         exception if {@code fromKey} or {@code toKey}
   *         cannot be compared to keys currently in the map.
   * @throws NullPointerException if {@code fromKey} or {@code toKey}
   *         is null and this map does not permit null keys
   * @throws IllegalArgumentException if {@code fromKey} is greater than
   *         {@code toKey}; or if this map itself has a restricted
   *         range, and {@code fromKey} or {@code toKey} lies
   *         outside the bounds of the range
   */
  AtomicNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey,   boolean toInclusive);

  @Override
  default AtomicSortedMap<K, V> headMap(K toKey) {
    return headMap(toKey, false);
  }

  /**
   * Returns a view of the portion of this map whose keys are less than (or
   * equal to, if {@code inclusive} is true) {@code toKey}.  The returned
   * map is backed by this map, so changes in the returned map are reflected
   * in this map, and vice-versa.  The returned map supports all optional
   * map operations that this map supports.
   *
   * <p>The returned map will throw an {@code IllegalArgumentException}
   * on an attempt to insert a key outside its range.
   *
   * @param toKey high endpoint of the keys in the returned map
   * @param inclusive {@code true} if the high endpoint
   *        is to be included in the returned view
   * @return a view of the portion of this map whose keys are less than
   *         (or equal to, if {@code inclusive} is true) {@code toKey}
   * @throws ClassCastException if {@code toKey} is not compatible
   *         with this map's comparator (or, if the map has no comparator,
   *         if {@code toKey} does not implement {@link Comparable}).
   *         Implementations may, but are not required to, throw this
   *         exception if {@code toKey} cannot be compared to keys
   *         currently in the map.
   * @throws NullPointerException if {@code toKey} is null
   *         and this map does not permit null keys
   * @throws IllegalArgumentException if this map itself has a
   *         restricted range, and {@code toKey} lies outside the
   *         bounds of the range
   */
  AtomicNavigableMap<K, V> headMap(K toKey, boolean inclusive);

  @Override
  default AtomicSortedMap<K, V> tailMap(K fromKey) {
    return tailMap(fromKey, true);
  }

  /**
   * Returns a view of the portion of this map whose keys are greater than (or
   * equal to, if {@code inclusive} is true) {@code fromKey}.  The returned
   * map is backed by this map, so changes in the returned map are reflected
   * in this map, and vice-versa.  The returned map supports all optional
   * map operations that this map supports.
   *
   * <p>The returned map will throw an {@code IllegalArgumentException}
   * on an attempt to insert a key outside its range.
   *
   * @param fromKey low endpoint of the keys in the returned map
   * @param inclusive {@code true} if the low endpoint
   *        is to be included in the returned view
   * @return a view of the portion of this map whose keys are greater than
   *         (or equal to, if {@code inclusive} is true) {@code fromKey}
   * @throws ClassCastException if {@code fromKey} is not compatible
   *         with this map's comparator (or, if the map has no comparator,
   *         if {@code fromKey} does not implement {@link Comparable}).
   *         Implementations may, but are not required to, throw this
   *         exception if {@code fromKey} cannot be compared to keys
   *         currently in the map.
   * @throws NullPointerException if {@code fromKey} is null
   *         and this map does not permit null keys
   * @throws IllegalArgumentException if this map itself has a
   *         restricted range, and {@code fromKey} lies outside the
   *         bounds of the range
   */
  AtomicNavigableMap<K, V> tailMap(K fromKey, boolean inclusive);

  @Override
  AsyncAtomicNavigableMap<K, V> async();
}
