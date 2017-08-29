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
package io.atomix.primitives.map;

import io.atomix.primitives.DistributedPrimitive;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * A distributed, eventually consistent map.
 * <p>
 * This map does not offer read after writes consistency. Operations are
 * serialized via the timestamps issued by the clock service. If two updates
 * are in conflict, the update with the more recent timestamp will endure.
 * </p><p>
 * The interface is mostly similar to {@link Map} with some minor
 * semantic changes and the addition of a listener framework (because the map
 * can be mutated by clients on other instances, not only through the local Java
 * API).
 * </p><p>
 * Clients are expected to register an
 * {@link EventuallyConsistentMapListener} if they
 * are interested in receiving notifications of update to the map.
 * </p><p>
 * Null values are not allowed in this map.
 * </p>
 */
public interface EventuallyConsistentMap<K, V> extends DistributedPrimitive {

  @Override
  default DistributedPrimitive.Type primitiveType() {
    return DistributedPrimitive.Type.EVENTUALLY_CONSISTENT_MAP;
  }

  /**
   * Returns the number of key-value mappings in this map.
   *
   * @return number of key-value mappings
   */
  int size();

  /**
   * Returns true if this map is empty.
   *
   * @return true if this map is empty, otherwise false
   */
  boolean isEmpty();

  /**
   * Returns true if the map contains a mapping for the specified key.
   *
   * @param key the key to check if this map contains
   * @return true if this map has a mapping for the key, otherwise false
   */
  boolean containsKey(K key);

  /**
   * Returns true if the map contains a mapping from any key to the specified
   * value.
   *
   * @param value the value to check if this map has a mapping for
   * @return true if this map has a mapping to this value, otherwise false
   */
  boolean containsValue(V value);

  /**
   * Returns the value mapped to the specified key.
   *
   * @param key the key to look up in this map
   * @return the value mapped to the key, or null if no mapping is found
   */
  V get(K key);

  /**
   * Associates the specified value to the specified key in this map.
   * <p>
   * Note: this differs from the specification of {@link Map}
   * because it does not return the previous value associated with the key.
   * Clients are expected to register an
   * {@link EventuallyConsistentMapListener} if
   * they are interested in receiving notification of updates to the map.
   * </p><p>
   * Null values are not allowed in the map.
   * </p>
   *
   * @param key   the key to add a mapping for in this map
   * @param value the value to associate with the key in this map
   */
  void put(K key, V value);

  /**
   * Removes the mapping associated with the specified key from the map.
   * <p>
   * Clients are expected to register an {@link EventuallyConsistentMapListener} if
   * they are interested in receiving notification of updates to the map.
   * </p>
   *
   * @param key the key to remove the mapping for
   * @return previous value associated with key, or null if there was no mapping for key.
   */
  V remove(K key);

  /**
   * Removes the given key-value mapping from the map, if it exists.
   * <p>
   * This actually means remove any values up to and including the timestamp
   * given by the map's timestampProvider.
   * Any mappings that produce an earlier timestamp than this given key-value
   * pair will be removed, and any mappings that produce a later timestamp
   * will supersede this remove.
   * </p><p>
   * Note: this differs from the specification of {@link Map}
   * because it does not return a boolean indication whether a value was removed.
   * Clients are expected to register an
   * {@link EventuallyConsistentMapListener} if
   * they are interested in receiving notification of updates to the map.
   * </p>
   *
   * @param key   the key to remove the mapping for
   * @param value the value mapped to the key
   */
  void remove(K key, V value);

  /**
   * Attempts to compute a mapping for the specified key and its current mapped
   * value (or null if there is no current mapping).
   * <p>
   * If the function returns null, the mapping is removed (or remains absent if initially absent).
   *
   * @param key               map key
   * @param recomputeFunction function to recompute a new value
   * @return new value
   */
  V compute(K key, BiFunction<K, V, V> recomputeFunction);

  /**
   * Adds mappings for all key-value pairs in the specified map to this map.
   * <p>
   * This will be more efficient in communication than calling individual put
   * operations.
   * </p>
   *
   * @param m a map of values to add to this map
   */
  void putAll(Map<? extends K, ? extends V> m);

  /**
   * Removes all mappings from this map.
   */
  void clear();

  /**
   * Returns a set of the keys in this map. Changes to the set are not
   * reflected back to the map.
   *
   * @return set of keys in the map
   */
  Set<K> keySet();

  /**
   * Returns a collections of values in this map. Changes to the collection
   * are not reflected back to the map.
   *
   * @return collection of values in the map
   */
  Collection<V> values();

  /**
   * Returns a set of mappings contained in this map. Changes to the set are
   * not reflected back to the map.
   *
   * @return set of key-value mappings in this map
   */
  Set<Map.Entry<K, V>> entrySet();

  /**
   * Adds the specified listener to the map which will be notified whenever
   * the mappings in the map are changed.
   *
   * @param listener listener to register for events
   */
  void addListener(EventuallyConsistentMapListener<K, V> listener);

  /**
   * Removes the specified listener from the map such that it will no longer
   * receive change notifications.
   *
   * @param listener listener to deregister for events
   */
  void removeListener(EventuallyConsistentMapListener<K, V> listener);
}
