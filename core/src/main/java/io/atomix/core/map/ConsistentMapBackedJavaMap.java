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

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import io.atomix.utils.time.Versioned;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Standard java {@link Map} backed by a {@link ConsistentMap}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public final class ConsistentMapBackedJavaMap<K, V> implements Map<K, V> {

  private final ConsistentMap<K, V> backingMap;

  public ConsistentMapBackedJavaMap(ConsistentMap<K, V> backingMap) {
    this.backingMap = backingMap;
  }

  @Override
  public int size() {
    return backingMap.size();
  }

  @Override
  public boolean isEmpty() {
    return backingMap.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return backingMap.containsKey((K) key);
  }

  @Override
  public boolean containsValue(Object value) {
    return backingMap.containsValue((V) value);
  }

  @Override
  public V get(Object key) {
    return Versioned.valueOrNull(backingMap.get((K) key));
  }

  @Override
  public V getOrDefault(Object key, V defaultValue) {
    return Versioned.valueOrElse(backingMap.get((K) key), defaultValue);
  }

  @Override
  public V put(K key, V value) {
    return Versioned.valueOrNull(backingMap.put(key, value));
  }

  @Override
  public V putIfAbsent(K key, V value) {
    return Versioned.valueOrNull(backingMap.putIfAbsent(key, value));
  }

  @Override
  public V remove(Object key) {
    return Versioned.valueOrNull(backingMap.remove((K) key));
  }

  @Override
  public boolean remove(Object key, Object value) {
    return backingMap.remove((K) key, (V) value);
  }

  @Override
  public V replace(K key, V value) {
    return Versioned.valueOrNull(backingMap.replace(key, value));
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return backingMap.replace(key, oldValue, newValue);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    m.forEach((k, v) -> {
      backingMap.put(k, v);
    });
  }

  @Override
  public void clear() {
    backingMap.clear();
  }

  @Override
  public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return Versioned.valueOrNull(backingMap.compute(key, remappingFunction));
  }

  @Override
  public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return Versioned.valueOrNull(backingMap.computeIfAbsent(key, mappingFunction));
  }

  @Override
  public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return Versioned.valueOrNull(backingMap.computeIfPresent(key, remappingFunction));
  }

  @Override
  public Set<K> keySet() {
    return backingMap.keySet();
  }

  @Override
  public Collection<V> values() {
    return Collections2.transform(backingMap.values(), v -> v.value());
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return backingMap.entrySet()
        .stream()
        .map(entry -> Maps.immutableEntry(entry.getKey(), entry.getValue().value()))
        .collect(Collectors.toSet());
  }

  @Override
  public String toString() {
    // Map like output
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    Iterator<Entry<K, Versioned<V>>> it = backingMap.entrySet().iterator();
    while (it.hasNext()) {
      Entry<K, Versioned<V>> entry = it.next();
      sb.append(entry.getKey()).append('=').append(entry.getValue().value());
      if (it.hasNext()) {
        sb.append(',').append(' ');
      }
    }
    sb.append('}');
    return sb.toString();
  }

  @Override
  public void forEach(BiConsumer<? super K, ? super V> action) {
    entrySet().forEach(e -> action.accept(e.getKey(), e.getValue()));
  }

  @Override
  public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
    return computeIfPresent(key, (k, v) -> v == null ? value : remappingFunction.apply(v, value));
  }
}