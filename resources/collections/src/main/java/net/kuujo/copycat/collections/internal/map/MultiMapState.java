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

import net.kuujo.copycat.raft.Consistency;
import net.kuujo.copycat.state.Read;
import net.kuujo.copycat.state.Write;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Asynchronous multimap status.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface MultiMapState<K, V> {

  /**
   * Returns the map size.
   */
  @Read(consistency=Consistency.DEFAULT)
  int size();

  /**
   * Returns a boolean indicating whether the map is empty.
   */
  @Read(consistency=Consistency.DEFAULT)
  boolean isEmpty();

  /**
   * Returns a boolean indicating whether the map contains a key.
   */
  @Read(consistency=Consistency.DEFAULT)
  boolean containsKey(K key);

  /**
   * Returns a boolean indicating whether the map contains a value.
   */
  @Read(consistency=Consistency.DEFAULT)
  boolean containsValue(V value);

  /**
   * Returns a boolean indicating whether the map contains an entry.
   */
  @Read(consistency=Consistency.DEFAULT)
  boolean containsEntry(K key, V value);

  /**
   * Gets a value from the map.
   */
  @Read(consistency=Consistency.DEFAULT)
  Collection<V> get(K key);

  /**
   * Puts a value in the map.
   */
  @Write
  Collection<V> put(K key, V value);

  /**
   * Removes a key from the map.
   */
  @Write
  Collection<V> remove(K key);

  /**
   * Removes an entry from the map.
   */
  @Write
  boolean remove(K key, V value);

  /**
   * Puts a collection of values in the map.
   */
  @Write
  void putAll(Map<? extends K, ? extends Collection<V>> m);

  /**
   * Clears the map.
   */
  @Write
  void clear();

  /**
   * Returns a set of keys in the map.
   */
  @Read(consistency=Consistency.DEFAULT)
  Set<K> keySet();

  /**
   * Returns a set of values in the map.
   */
  @Read(consistency=Consistency.DEFAULT)
  Collection<V> values();

  /**
   * Returns a set of entries in the map.
   */
  @Read(consistency=Consistency.DEFAULT)
  Set<Map.Entry<K, V>> entrySet();

  /**
   * Gets a key from the map or returns a default value.
   */
  @Read(consistency=Consistency.DEFAULT)
  Collection<V> getOrDefault(K key, Collection<V> defaultValue);

  /**
   * Replaces a set of keys in the map.
   */
  @Write
  void replaceAll(BiFunction<? super K, ? super Collection<V>, ? extends Collection<V>> function);

  /**
   * Replaces a value in the map.
   */
  @Write
  boolean replace(K key, V oldValue, V newValue);

  /**
   * Replaces a key in the map.
   */
  @Write
  Collection<V> replace(K key, Collection<V> value);

}
