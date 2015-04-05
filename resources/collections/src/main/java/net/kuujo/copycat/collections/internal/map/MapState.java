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

import net.kuujo.copycat.state.Read;
import net.kuujo.copycat.state.Write;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Asynchronous map status.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface MapState<K, V> extends Map<K, V> {

  @Override
  @Read
  int size();

  @Override
  @Read
  boolean isEmpty();

  @Override
  @Read
  boolean containsKey(Object key);

  @Override
  @Read
  boolean containsValue(Object value);

  @Override
  @Read
  V get(Object key);

  @Override
  @Write
  V put(K key, V value);

  @Override
  @Write
  V remove(Object key);

  @Override
  @Write
  void putAll(Map<? extends K, ? extends V> m);

  @Override
  @Write
  void clear();

  @Override
  @Read
  Set<K> keySet();

  @Override
  @Read
  Collection<V> values();

  @Override
  @Read
  Set<Entry<K, V>> entrySet();

  @Override
  @Read
  V getOrDefault(Object key, V defaultValue);

  @Override
  @Write
  void replaceAll(BiFunction<? super K, ? super V, ? extends V> function);

  @Override
  @Write
  V putIfAbsent(K key, V value);

  @Override
  @Write
  boolean remove(Object key, Object value);

  @Override
  @Write
  boolean replace(K key, V oldValue, V newValue);

  @Override
  @Write
  V replace(K key, V value);

  @Override
  @Write
  V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction);

  @Override
  @Write
  V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

  @Override
  @Write
  V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

  @Override
  @Write
  V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction);

}
