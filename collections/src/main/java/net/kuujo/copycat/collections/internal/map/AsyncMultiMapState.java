/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.collections.internal.map;

import net.kuujo.copycat.StateContext;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Asynchronous multimap state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncMultiMapState<K, V> {

  /**
   * Sets a key entry in the map.
   *
   * @param key The key to set.
   * @param value The entry to set
   * @param context The map state context.
   */
  boolean put(K key, V value, StateContext<AsyncMultiMapState<K, V>> context);

  /**
   * Gets a key entry in the map.
   *
   * @param key The key to get.
   * @param context The map state context.
   */
  Collection<V> get(K key, StateContext<AsyncMultiMapState<K, V>> context);

  /**
   * Removes a key from the map.
   *
   * @param key The key to remove.
   * @param context The map state context.
   */
  Collection<V> remove(K key, StateContext<AsyncMultiMapState<K, V>> context);

  /**
   * Removes a entry from a key in the map.
   *
   * @param key The key from which to remove the entry.
   * @param value The entry to remove.
   * @param context The map state context.
   */
  boolean remove(K key, V value, StateContext<AsyncMultiMapState<K, V>> context);

  /**
   * Checks if the map contains a key.
   *
   * @param key The key to check.
   * @param context The map state context.
   */
  boolean containsKey(K key, StateContext<AsyncMultiMapState<K, V>> context);

  /**
   * Checks if the map contains a entry.
   *
   * @param value The entry to check.
   * @param context The map state context.
   */
  boolean containsValue(V value, StateContext<AsyncMultiMapState<K, V>> context);

  /**
   * Checks if the map contains a key/entry pair.
   *
   * @param key The key to check.
   * @param value The entry to check.
   * @param context The map state context.
   */
  boolean containsEntry(K key, V value, StateContext<AsyncMultiMapState<K, V>> context);

  /**
   * Gets a set of keys in the map.
   *
   * @param context The map state context.
   */
  Set<K> keySet(StateContext<AsyncMultiMapState<K, V>> context);

  /**
   * Gets a set of entries in the map.
   *
   * @param context The map state context.
   */
  Set<Map.Entry<K, Collection<V>>> entrySet(StateContext<AsyncMultiMapState<K, V>> context);

  /**
   * Gets a collection of values in the map.
   *
   * @param context The map state context.
   */
  Collection<V> values(StateContext<AsyncMultiMapState<K, V>> context);

  /**
   * Gets the current size of the map.
   *
   * @param context The map state context.
   */
  int size(StateContext<AsyncMultiMapState<K, V>> context);

  /**
   * Checks whether the map is empty.
   *
   * @param context The map state context.
   */
  boolean isEmpty(StateContext<AsyncMultiMapState<K, V>> context);

  /**
   * Clears all keys from the map.
   *
   * @param context The map state context.
   */
  void clear(StateContext<AsyncMultiMapState<K, V>> context);

}
