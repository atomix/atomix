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

import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous atomic navigable map.
 */
public interface AsyncAtomicNavigableMap<K extends Comparable<K>, V> extends AsyncAtomicSortedMap<K, V> {

  /**
   * Returns a key-value mapping associated with the greatest key
   * strictly less than the given key, or {@code null} if there is
   * no such key.
   *
   * @param key the key
   * @return an entry with the greatest key less than {@code key},
   *         or {@code null} if there is no such key
   * @throws ClassCastException if the specified key cannot be compared
   *         with the keys currently in the map
   * @throws NullPointerException if the specified key is null
   *         and this map does not permit null keys
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> lowerEntry(K key);

  /**
   * Returns the greatest key strictly less than the given key, or
   * {@code null} if there is no such key.
   *
   * @param key the key
   * @return the greatest key less than {@code key},
   *         or {@code null} if there is no such key
   * @throws ClassCastException if the specified key cannot be compared
   *         with the keys currently in the map
   * @throws NullPointerException if the specified key is null
   *         and this map does not permit null keys
   */
  CompletableFuture<K> lowerKey(K key);

  /**
   * Returns a key-value mapping associated with the greatest key
   * less than or equal to the given key, or {@code null} if there
   * is no such key.
   *
   * @param key the key
   * @return an entry with the greatest key less than or equal to
   *         {@code key}, or {@code null} if there is no such key
   * @throws ClassCastException if the specified key cannot be compared
   *         with the keys currently in the map
   * @throws NullPointerException if the specified key is null
   *         and this map does not permit null keys
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> floorEntry(K key);

  /**
   * Returns the greatest key less than or equal to the given key,
   * or {@code null} if there is no such key.
   *
   * @param key the key
   * @return the greatest key less than or equal to {@code key},
   *         or {@code null} if there is no such key
   * @throws ClassCastException if the specified key cannot be compared
   *         with the keys currently in the map
   * @throws NullPointerException if the specified key is null
   *         and this map does not permit null keys
   */
  CompletableFuture<K> floorKey(K key);

  /**
   * Returns a key-value mapping associated with the least key
   * greater than or equal to the given key, or {@code null} if
   * there is no such key.
   *
   * @param key the key
   * @return an entry with the least key greater than or equal to
   *         {@code key}, or {@code null} if there is no such key
   * @throws ClassCastException if the specified key cannot be compared
   *         with the keys currently in the map
   * @throws NullPointerException if the specified key is null
   *         and this map does not permit null keys
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> ceilingEntry(K key);

  /**
   * Returns the least key greater than or equal to the given key,
   * or {@code null} if there is no such key.
   *
   * @param key the key
   * @return the least key greater than or equal to {@code key},
   *         or {@code null} if there is no such key
   * @throws ClassCastException if the specified key cannot be compared
   *         with the keys currently in the map
   * @throws NullPointerException if the specified key is null
   *         and this map does not permit null keys
   */
  CompletableFuture<K> ceilingKey(K key);

  /**
   * Returns a key-value mapping associated with the least key
   * strictly greater than the given key, or {@code null} if there
   * is no such key.
   *
   * @param key the key
   * @return an entry with the least key greater than {@code key},
   *         or {@code null} if there is no such key
   * @throws ClassCastException if the specified key cannot be compared
   *         with the keys currently in the map
   * @throws NullPointerException if the specified key is null
   *         and this map does not permit null keys
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> higherEntry(K key);

  /**
   * Returns the least key strictly greater than the given key, or
   * {@code null} if there is no such key.
   *
   * @param key the key
   * @return the least key greater than {@code key},
   *         or {@code null} if there is no such key
   * @throws ClassCastException if the specified key cannot be compared
   *         with the keys currently in the map
   * @throws NullPointerException if the specified key is null
   *         and this map does not permit null keys
   */
  CompletableFuture<K> higherKey(K key);

  /**
   * Returns a key-value mapping associated with the least
   * key in this map, or {@code null} if the map is empty.
   *
   * @return an entry with the least key,
   *         or {@code null} if this map is empty
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> firstEntry();

  /**
   * Returns a key-value mapping associated with the greatest
   * key in this map, or {@code null} if the map is empty.
   *
   * @return an entry with the greatest key,
   *         or {@code null} if this map is empty
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> lastEntry();

  /**
   * Removes and returns a key-value mapping associated with
   * the least key in this map, or {@code null} if the map is empty.
   *
   * @return the removed first entry of this map,
   *         or {@code null} if this map is empty
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> pollFirstEntry();

  /**
   * Removes and returns a key-value mapping associated with
   * the greatest key in this map, or {@code null} if the map is empty.
   *
   * @return the removed last entry of this map,
   *         or {@code null} if this map is empty
   */
  CompletableFuture<Map.Entry<K, Versioned<V>>> pollLastEntry();

  @Override
  default AsyncAtomicSortedMap<K, V> subMap(K fromKey, K toKey) {
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
  AsyncAtomicNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey,   boolean toInclusive);

  @Override
  default AsyncAtomicSortedMap<K, V> headMap(K toKey) {
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
  AsyncAtomicNavigableMap<K, V> headMap(K toKey, boolean inclusive);

  @Override
  default AsyncAtomicSortedMap<K, V> tailMap(K fromKey) {
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
  AsyncAtomicNavigableMap<K, V> tailMap(K fromKey, boolean inclusive);

  /**
   * Returns a reverse order view of the mappings contained in this map.
   * The descending map is backed by this map, so changes to the map are
   * reflected in the descending map, and vice-versa.  If either map is
   * modified while an iteration over a collection view of either map
   * is in progress (except through the iterator's own {@code remove}
   * operation), the results of the iteration are undefined.
   *
   * <p>The returned map has an ordering equivalent to
   * <tt>{@link Collections#reverseOrder(Comparator) Collections.reverseOrder}(comparator())</tt>.
   * The expression {@code m.descendingMap().descendingMap()} returns a
   * view of {@code m} essentially equivalent to {@code m}.
   *
   * @return a reverse order view of this map
   */
  AsyncAtomicNavigableMap<K, V> descendingMap();

  /**
   * Returns a {@link NavigableSet} view of the keys contained in this map.
   * The set's iterator returns the keys in ascending order.
   * The set is backed by the map, so changes to the map are reflected in
   * the set, and vice-versa.  If the map is modified while an iteration
   * over the set is in progress (except through the iterator's own {@code
   * remove} operation), the results of the iteration are undefined.  The
   * set supports element removal, which removes the corresponding mapping
   * from the map, via the {@code Iterator.remove}, {@code Set.remove},
   * {@code removeAll}, {@code retainAll}, and {@code clear} operations.
   * It does not support the {@code add} or {@code addAll} operations.
   *
   * @return a navigable set view of the keys in this map
   */
  AsyncDistributedNavigableSet<K> navigableKeySet();

  /**
   * Returns a reverse order {@link NavigableSet} view of the keys contained in this map.
   * The set's iterator returns the keys in descending order.
   * The set is backed by the map, so changes to the map are reflected in
   * the set, and vice-versa.  If the map is modified while an iteration
   * over the set is in progress (except through the iterator's own {@code
   * remove} operation), the results of the iteration are undefined.  The
   * set supports element removal, which removes the corresponding mapping
   * from the map, via the {@code Iterator.remove}, {@code Set.remove},
   * {@code removeAll}, {@code retainAll}, and {@code clear} operations.
   * It does not support the {@code add} or {@code addAll} operations.
   *
   * @return a reverse order navigable set view of the keys in this map
   */
  AsyncDistributedNavigableSet<K> descendingKeySet();

  @Override
  default AtomicNavigableMap<K, V> sync() {
    return sync(Duration.ofMillis(DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  AtomicNavigableMap<K, V> sync(Duration operationTimeout);
}
