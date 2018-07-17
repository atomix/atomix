/*
 * Copyright 2015-present Open Networking Foundation
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

import com.google.common.util.concurrent.MoreExecutors;
import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.map.impl.MapUpdate;
import io.atomix.core.transaction.Transactional;
import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitive;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A distributed, strongly consistent map whose methods are all executed asynchronously.
 * <p>
 * This map offers strong read-after-update (where update == create/update/delete)
 * consistency. All operations to the map are serialized and applied in a consistent
 * manner.
 * <p>
 * The stronger consistency comes at the expense of availability in
 * the event of a network partition. A network partition can be either due to
 * a temporary disruption in network connectivity between participating nodes
 * or due to a node being temporarily down.
 * </p><p>
 * All values stored in this map are {@link Versioned versioned} and the API
 * supports optimistic concurrency by allowing conditional updates that take into
 * consideration the version or value that was previously read.
 * </p><p>
 * This map does not allow null values. All methods can throw a ConsistentMapException
 * (which extends {@code RuntimeException}) to indicate failures.
 * <p>
 * All methods of this interface return a {@link CompletableFuture future} immediately
 * after a successful invocation. The operation itself is executed asynchronous and
 * the returned future will be {@link CompletableFuture#complete completed} when the
 * operation finishes.
 */
public interface AsyncAtomicMap<K, V> extends AsyncPrimitive, Transactional<MapUpdate<K, V>> {

  @Override
  default CompletableFuture<Void> delete() {
    return clear();
  }

  /**
   * Returns the number of entries in the map.
   *
   * @return a future for map size.
   */
  CompletableFuture<Integer> size();

  /**
   * Returns true if the map is empty.
   *
   * @return a future whose value will be true if map has no entries, false otherwise.
   */
  default CompletableFuture<Boolean> isEmpty() {
    return size().thenApply(s -> s == 0);
  }

  /**
   * Returns true if this map contains a mapping for the specified key.
   *
   * @param key key
   * @return a future whose value will be true if map contains key, false otherwise.
   */
  CompletableFuture<Boolean> containsKey(K key);

  /**
   * Returns true if this map contains the specified value.
   *
   * @param value value
   * @return a future whose value will be true if map contains value, false otherwise.
   */
  CompletableFuture<Boolean> containsValue(V value);

  /**
   * Returns the value (and version) to which the specified key is mapped, or null if this
   * map contains no mapping for the key.
   *
   * @param key the key whose associated value (and version) is to be returned
   * @return a future value (and version) to which the specified key is mapped, or null if
   * this map contains no mapping for the key
   */
  CompletableFuture<Versioned<V>> get(K key);

  /**
   * Returns a map of the values associated with the {@code keys} in this map. The returned map
   * will only contain entries which already exist in the map.
   * <p>
   * Note that duplicate elements in {@code keys}, as determined by {@link Object#equals}, will be
   * ignored.
   *
   * @param keys the keys whose associated values are to be returned
   * @return the unmodifiable mapping of keys to values for the specified keys found in the map
   */
  CompletableFuture<Map<K, Versioned<V>>> getAllPresent(Iterable<K> keys);

  /**
   * Returns the value (and version) to which the specified key is mapped, or the provided
   * default value if this map contains no mapping for the key.
   *
   * @param key          the key whose associated value (and version) is to be returned
   * @param defaultValue the default value to return if the key is not set
   * @return a future value (and version) to which the specified key is mapped, or null if
   * this map contains no mapping for the key
   */
  CompletableFuture<Versioned<V>> getOrDefault(K key, V defaultValue);

  /**
   * If the specified key is not already associated with a value (or is mapped to null),
   * attempts to compute its value using the given mapping function and enters it into
   * this map unless null.
   * If a conflicting concurrent modification attempt is detected, the returned future
   * will be completed exceptionally with ConsistentMapException.ConcurrentModification.
   *
   * @param key             key with which the specified value is to be associated
   * @param mappingFunction the function to compute a value
   * @return the current (existing or computed) value associated with the specified key,
   * or null if the computed value is null
   */
  default CompletableFuture<Versioned<V>> computeIfAbsent(
      K key, Function<? super K, ? extends V> mappingFunction) {
    return computeIf(key, Objects::isNull, (k, v) -> mappingFunction.apply(k));
  }

  /**
   * If the value for the specified key is present and non-null, attempts to compute a new
   * mapping given the key and its current mapped value.
   * If the computed value is null, the current mapping will be removed from the map.
   * If a conflicting concurrent modification attempt is detected, the returned future
   * will be completed exceptionally with ConsistentMapException.ConcurrentModification.
   *
   * @param key               key with which the specified value is to be associated
   * @param remappingFunction the function to compute a value
   * @return the new value associated with the specified key, or null if computed value is null
   */
  default CompletableFuture<Versioned<V>> computeIfPresent(
      K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return computeIf(key, Objects::nonNull, remappingFunction);
  }

  /**
   * Attempts to compute a mapping for the specified key and its current mapped value (or
   * null if there is no current mapping).
   * If the computed value is null, the current mapping (if one exists) will be removed from the map.
   * If a conflicting concurrent modification attempt is detected, the returned future
   * will be completed exceptionally with ConsistentMapException.ConcurrentModification.
   *
   * @param key               key with which the specified value is to be associated
   * @param remappingFunction the function to compute a value
   * @return the new value associated with the specified key, or null if computed value is null
   */
  default CompletableFuture<Versioned<V>> compute(
      K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return computeIf(key, v -> true, remappingFunction);
  }

  /**
   * If the value for the specified key satisfies a condition, attempts to compute a new
   * mapping given the key and its current mapped value.
   * If the computed value is null, the current mapping will be removed from the map.
   * If a conflicting concurrent modification attempt is detected, the returned future
   * will be completed exceptionally with ConsistentMapException.ConcurrentModification.
   *
   * @param key               key with which the specified value is to be associated
   * @param condition         condition that should evaluate to true for the computation to proceed
   * @param remappingFunction the function to compute a value
   * @return the new value associated with the specified key, or the old value if condition evaluates to false
   */
  CompletableFuture<Versioned<V>> computeIf(
      K key, Predicate<? super V> condition, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

  /**
   * Associates the specified value with the specified key in this map (optional operation).
   * If the map previously contained a mapping for the key, the old value is replaced by the
   * specified value.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value (and version) associated with key, or null if there was
   * no mapping for key.
   */
  default CompletableFuture<Versioned<V>> put(K key, V value) {
    return put(key, value, Duration.ZERO);
  }

  /**
   * Associates the specified value with the specified key in this map (optional operation).
   * If the map previously contained a mapping for the key, the old value is replaced by the
   * specified value.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @param ttl   the time to live after which to remove the value
   * @return the previous value (and version) associated with key, or null if there was
   * no mapping for key.
   */
  CompletableFuture<Versioned<V>> put(K key, V value, Duration ttl);

  /**
   * Associates the specified value with the specified key in this map (optional operation).
   * If the map previously contained a mapping for the key, the old value is replaced by the
   * specified value.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return new value.
   */
  default CompletableFuture<Versioned<V>> putAndGet(K key, V value) {
    return putAndGet(key, value, Duration.ZERO);
  }

  /**
   * Associates the specified value with the specified key in this map (optional operation).
   * If the map previously contained a mapping for the key, the old value is replaced by the
   * specified value.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @param ttl   the time to live after which to remove the value
   * @return new value.
   */
  CompletableFuture<Versioned<V>> putAndGet(K key, V value, Duration ttl);

  /**
   * Removes the mapping for a key from this map if it is present (optional operation).
   *
   * @param key key whose value is to be removed from the map
   * @return the value (and version) to which this map previously associated the key,
   * or null if the map contained no mapping for the key.
   */
  CompletableFuture<Versioned<V>> remove(K key);

  /**
   * Removes all of the mappings from this map (optional operation).
   * The map will be empty after this call returns.
   *
   * @return future that will be successfully completed when the map is cleared
   */
  CompletableFuture<Void> clear();

  /**
   * Returns a Set view of the keys contained in this map.
   * This method differs from the behavior of java.util.Map.keySet() in that
   * what is returned is a unmodifiable snapshot view of the keys in the ConsistentMap.
   * Attempts to modify the returned set, whether direct or via its iterator,
   * result in an UnsupportedOperationException.
   *
   * @return a set of the keys contained in this map
   */
  AsyncDistributedSet<K> keySet();

  /**
   * Returns the collection of values (and associated versions) contained in this map.
   * This method differs from the behavior of java.util.Map.values() in that
   * what is returned is a unmodifiable snapshot view of the values in the ConsistentMap.
   * Attempts to modify the returned collection, whether direct or via its iterator,
   * result in an UnsupportedOperationException.
   *
   * @return a collection of the values (and associated versions) contained in this map
   */
  AsyncDistributedCollection<Versioned<V>> values();

  /**
   * Returns the set of entries contained in this map.
   * This method differs from the behavior of java.util.Map.entrySet() in that
   * what is returned is a unmodifiable snapshot view of the entries in the ConsistentMap.
   * Attempts to modify the returned set, whether direct or via its iterator,
   * result in an UnsupportedOperationException.
   *
   * @return set of entries contained in this map.
   */
  AsyncDistributedSet<Entry<K, Versioned<V>>> entrySet();

  /**
   * If the specified key is not already associated with a value associates
   * it with the given value and returns null, else behaves as a get
   * returning the existing mapping without making any changes.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with the specified key or null
   * if key does not already mapped to a value.
   */
  default CompletableFuture<Versioned<V>> putIfAbsent(K key, V value) {
    return putIfAbsent(key, value, Duration.ZERO);
  }

  /**
   * If the specified key is not already associated with a value associates
   * it with the given value and returns null, else behaves as a get
   * returning the existing mapping without making any changes.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @param ttl   the time to live after which to remove the value
   * @return the previous value associated with the specified key or null
   * if key does not already mapped to a value.
   */
  CompletableFuture<Versioned<V>> putIfAbsent(K key, V value, Duration ttl);

  /**
   * Removes the entry for the specified key only if it is currently
   * mapped to the specified value.
   *
   * @param key   key with which the specified value is associated
   * @param value value expected to be associated with the specified key
   * @return true if the value was removed
   */
  CompletableFuture<Boolean> remove(K key, V value);

  /**
   * Removes the entry for the specified key only if its current
   * version in the map is equal to the specified version.
   *
   * @param key     key with which the specified version is associated
   * @param version version expected to be associated with the specified key
   * @return true if the value was removed
   */
  CompletableFuture<Boolean> remove(K key, long version);

  /**
   * Replaces the entry for the specified key only if there is any value
   * which associated with specified key.
   *
   * @param key   key with which the specified value is associated
   * @param value value expected to be associated with the specified key
   * @return the previous value associated with the specified key or null
   */
  CompletableFuture<Versioned<V>> replace(K key, V value);

  /**
   * Replaces the entry for the specified key only if currently mapped
   * to the specified value.
   *
   * @param key      key with which the specified value is associated
   * @param oldValue value expected to be associated with the specified key
   * @param newValue value to be associated with the specified key
   * @return true if the value was replaced
   */
  CompletableFuture<Boolean> replace(K key, V oldValue, V newValue);

  /**
   * Replaces the entry for the specified key only if it is currently mapped to the
   * specified version.
   *
   * @param key        key key with which the specified value is associated
   * @param oldVersion version expected to be associated with the specified key
   * @param newValue   value to be associated with the specified key
   * @return true if the value was replaced
   */
  CompletableFuture<Boolean> replace(K key, long oldVersion, V newValue);

  /**
   * Registers the specified listener to be notified whenever the map is updated.
   *
   * @param listener listener to notify about map events
   * @return future that will be completed when the operation finishes
   */
  default CompletableFuture<Void> addListener(AtomicMapEventListener<K, V> listener) {
    return addListener(listener, MoreExecutors.directExecutor());
  }

  /**
   * Registers the specified listener to be notified whenever the map is updated.
   *
   * @param listener listener to notify about map events
   * @param executor executor to use for handling incoming map events
   * @return future that will be completed when the operation finishes
   */
  CompletableFuture<Void> addListener(AtomicMapEventListener<K, V> listener, Executor executor);

  /**
   * Unregisters the specified listener such that it will no longer
   * receive map change notifications.
   *
   * @param listener listener to unregister
   * @return future that will be completed when the operation finishes
   */
  CompletableFuture<Void> removeListener(AtomicMapEventListener<K, V> listener);

  @Override
  default AtomicMap<K, V> sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  AtomicMap<K, V> sync(Duration operationTimeout);
}
