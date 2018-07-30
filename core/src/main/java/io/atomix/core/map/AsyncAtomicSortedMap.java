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
package io.atomix.core.map;

import java.time.Duration;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous atomic sorted map.
 */
public interface AsyncAtomicSortedMap<K extends Comparable<K>, V> extends AsyncAtomicMap<K, V> {

  /**
   * Returns the first (lowest) key currently in this map.
   *
   * @return the first (lowest) key currently in this map
   * @throws NoSuchElementException if this map is empty
   */
  CompletableFuture<K> firstKey();

  /**
   * Returns the last (highest) key currently in this map.
   *
   * @return the last (highest) key currently in this map
   * @throws NoSuchElementException if this map is empty
   */
  CompletableFuture<K> lastKey();

  /**
   * Returns a view of the portion of this map whose keys range from {@code fromKey}, inclusive, to {@code toKey},
   * exclusive.  (If {@code fromKey} and {@code toKey} are equal, the returned map is empty.)  The returned map is
   * backed by this map, so changes in the returned map are reflected in this map, and vice-versa. The returned map
   * supports all optional map operations that this map supports.
   *
   * <p>The returned map will throw an {@code IllegalArgumentException}
   * on an attempt to insert a key outside its range.
   *
   * @param fromKey low endpoint (inclusive) of the keys in the returned map
   * @param toKey high endpoint (exclusive) of the keys in the returned map
   * @return a view of the portion of this map whose keys range from {@code fromKey}, inclusive, to {@code toKey},
   *     exclusive
   * @throws ClassCastException if {@code fromKey} and {@code toKey} cannot be compared to one another using this
   *     map's comparator (or, if the map has no comparator, using natural ordering). Implementations may, but are not
   *     required to, throw this exception if {@code fromKey} or {@code toKey} cannot be compared to keys currently in
   *     the map.
   * @throws NullPointerException if {@code fromKey} or {@code toKey} is null and this map does not permit null
   *     keys
   * @throws IllegalArgumentException if {@code fromKey} is greater than {@code toKey}; or if this map itself has a
   *     restricted range, and {@code fromKey} or {@code toKey} lies outside the bounds of the range
   */
  AsyncAtomicSortedMap<K, V> subMap(K fromKey, K toKey);

  /**
   * Returns a view of the portion of this map whose keys are strictly less than {@code toKey}.  The returned map is
   * backed by this map, so changes in the returned map are reflected in this map, and vice-versa.  The returned map
   * supports all optional map operations that this map supports.
   *
   * <p>The returned map will throw an {@code IllegalArgumentException}
   * on an attempt to insert a key outside its range.
   *
   * @param toKey high endpoint (exclusive) of the keys in the returned map
   * @return a view of the portion of this map whose keys are strictly less than {@code toKey}
   * @throws ClassCastException if {@code toKey} is not compatible with this map's comparator (or, if the map has no
   *     comparator, if {@code toKey} does not implement {@link Comparable}). Implementations may, but are not required
   *     to, throw this exception if {@code toKey} cannot be compared to keys currently in the map.
   * @throws NullPointerException if {@code toKey} is null and this map does not permit null keys
   * @throws IllegalArgumentException if this map itself has a restricted range, and {@code toKey} lies outside the
   *     bounds of the range
   */
  AsyncAtomicSortedMap<K, V> headMap(K toKey);

  /**
   * Returns a view of the portion of this map whose keys are greater than or equal to {@code fromKey}.  The returned
   * map is backed by this map, so changes in the returned map are reflected in this map, and vice-versa.  The returned
   * map supports all optional map operations that this map supports.
   *
   * <p>The returned map will throw an {@code IllegalArgumentException}
   * on an attempt to insert a key outside its range.
   *
   * @param fromKey low endpoint (inclusive) of the keys in the returned map
   * @return a view of the portion of this map whose keys are greater than or equal to {@code fromKey}
   * @throws ClassCastException if {@code fromKey} is not compatible with this map's comparator (or, if the map has
   *     no comparator, if {@code fromKey} does not implement {@link Comparable}). Implementations may, but are not
   *     required to, throw this exception if {@code fromKey} cannot be compared to keys currently in the map.
   * @throws NullPointerException if {@code fromKey} is null and this map does not permit null keys
   * @throws IllegalArgumentException if this map itself has a restricted range, and {@code fromKey} lies outside
   *     the bounds of the range
   */
  AsyncAtomicSortedMap<K, V> tailMap(K fromKey);

  @Override
  default AtomicSortedMap<K, V> sync() {
    return sync(Duration.ofMillis(DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  AtomicSortedMap<K, V> sync(Duration operationTimeout);
}
