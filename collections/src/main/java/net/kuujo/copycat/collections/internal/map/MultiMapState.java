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

import net.kuujo.copycat.protocol.Consistency;
import net.kuujo.copycat.state.Command;
import net.kuujo.copycat.state.Initializer;
import net.kuujo.copycat.state.Query;
import net.kuujo.copycat.state.StateContext;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Asynchronous multimap state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface MultiMapState<K, V> {

  /**
   * Initializes the multimap state.
   *
   * @param context The multimap state context.
   */
  @Initializer
  public void init(StateContext<MultiMapState<K, V>> context);

  /**
   * Returns the map size.
   */
  @Query(consistency=Consistency.DEFAULT)
  int size();

  /**
   * Returns a boolean indicating whether the map is empty.
   */
  @Query(consistency=Consistency.DEFAULT)
  boolean isEmpty();

  /**
   * Returns a boolean indicating whether the map contains a key.
   */
  @Query(consistency=Consistency.DEFAULT)
  boolean containsKey(K key);

  /**
   * Returns a boolean indicating whether the map contains a value.
   */
  @Query(consistency=Consistency.DEFAULT)
  boolean containsValue(V value);

  /**
   * Returns a boolean indicating whether the map contains an entry.
   */
  @Query(consistency=Consistency.DEFAULT)
  boolean containsEntry(K key, V value);

  /**
   * Gets a value from the map.
   */
  @Query(consistency=Consistency.DEFAULT)
  Collection<V> get(K key);

  /**
   * Puts a value in the map.
   */
  @Command
  Collection<V> put(K key, V value);

  /**
   * Removes a key from the map.
   */
  @Command
  Collection<V> remove(K key);

  /**
   * Removes an entry from the map.
   */
  @Command
  boolean remove(K key, V value);

  /**
   * Puts a collection of values in the map.
   */
  @Command
  void putAll(Map<? extends K, ? extends Collection<V>> m);

  /**
   * Clears the map.
   */
  @Command
  void clear();

  /**
   * Returns a set of keys in the map.
   */
  @Query(consistency=Consistency.DEFAULT)
  Set<K> keySet();

  /**
   * Returns a set of values in the map.
   */
  @Query(consistency=Consistency.DEFAULT)
  Collection<V> values();

  /**
   * Returns a set of entries in the map.
   */
  @Query(consistency=Consistency.DEFAULT)
  Set<Map.Entry<K, V>> entrySet();

  /**
   * Gets a key from the map or returns a default value.
   */
  @Query(consistency=Consistency.DEFAULT)
  Collection<V> getOrDefault(K key, Collection<V> defaultValue);

  /**
   * Replaces a set of keys in the map.
   */
  @Command
  void replaceAll(BiFunction<? super K, ? super Collection<V>, ? extends Collection<V>> function);

  /**
   * Replaces a value in the map.
   */
  @Command
  boolean replace(K key, V oldValue, V newValue);

  /**
   * Replaces a key in the map.
   */
  @Command
  Collection<V> replace(K key, Collection<V> value);

}
