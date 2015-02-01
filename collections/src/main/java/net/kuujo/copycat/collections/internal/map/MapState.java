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

import net.kuujo.copycat.state.Command;
import net.kuujo.copycat.state.Initializer;
import net.kuujo.copycat.state.Query;
import net.kuujo.copycat.state.StateContext;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Asynchronous map state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface MapState<K, V> extends Map<K, V> {

  /**
   * Initializes the map state.
   *
   * @param context The map state context.
   */
  @Initializer
  public void init(StateContext<MapState<K, V>> context);

  @Override
  @Query
  int size();

  @Override
  @Query
  boolean isEmpty();

  @Override
  @Query
  boolean containsKey(Object key);

  @Override
  @Query
  boolean containsValue(Object value);

  @Override
  @Query
  V get(Object key);

  @Override
  @Command
  V put(K key, V value);

  @Override
  @Command
  V remove(Object key);

  @Override
  @Command
  void putAll(Map<? extends K, ? extends V> m);

  @Override
  @Command
  void clear();

  @Override
  @Query
  Set<K> keySet();

  @Override
  @Query
  Collection<V> values();

  @Override
  @Query
  Set<Entry<K, V>> entrySet();

  @Override
  @Query
  V getOrDefault(Object key, V defaultValue);

  @Override
  @Command
  void replaceAll(BiFunction<? super K, ? super V, ? extends V> function);

  @Override
  @Command
  V putIfAbsent(K key, V value);

  @Override
  @Command
  boolean remove(Object key, Object value);

  @Override
  @Command
  boolean replace(K key, V oldValue, V newValue);

  @Override
  @Command
  V replace(K key, V value);

  @Override
  @Command
  V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction);

  @Override
  @Command
  V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

  @Override
  @Command
  V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

  @Override
  @Command
  V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction);

}
