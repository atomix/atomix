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

import java.util.*;
import java.util.function.BiFunction;

/**
 * Default asynchronous multimap status.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultMultiMapState<K, V> implements MultiMapState<K, V> {
  private Map<K, Collection<V>> map = new HashMap<>();

  @Override
  public int size() {
    return map.size();
  }

  @Override
  public boolean isEmpty() {
    return map.isEmpty();
  }

  @Override
  public boolean containsKey(K key) {
    return map.containsKey(key);
  }

  @Override
  public boolean containsValue(V value) {
    for (Collection<V> values : map.values()) {
      if (values.contains(value)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean containsEntry(K key, V value) {
    return map.containsKey(key) && map.get(key).contains(value);
  }

  @Override
  public Collection<V> get(K key) {
    return map.get(key);
  }

  @Override
  public Collection<V> put(K key, V value) {
    Collection<V> values = map.get(key);
    if (values == null) {
      values = new ArrayList<>();
      map.put(key, values);
    }
    values.add(value);
    return values;
  }

  @Override
  public Collection<V> remove(K key) {
    return map.remove(key);
  }

  @Override
  public boolean remove(K key, V value) {
    Collection<V> values = map.get(key);
    if (values != null) {
      boolean result = values.remove(value);
      if (values.isEmpty()) {
        map.remove(key);
      }
      return result;
    }
    return false;
  }

  @Override
  public void putAll(Map<? extends K, ? extends Collection<V>> m) {
    map.putAll(m);
  }

  @Override
  public void clear() {
    map.clear();
  }

  @Override
  public Set<K> keySet() {
    return map.keySet();
  }

  @Override
  public Collection<V> values() {
    Collection<V> values = new ArrayList<>();
    map.values().forEach(values::addAll);
    return values;
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet() {
    Set<Map.Entry<K, V>> entries = new HashSet<>();
    for (Map.Entry<K, Collection<V>> entry : map.entrySet()) {
      entry.getValue().forEach(value -> entries.add(new AbstractMap.SimpleEntry<>(entry.getKey(), value)));
    }
    return entries;
  }

  @Override
  public Collection<V> getOrDefault(K key, Collection<V> defaultValue) {
    return map.getOrDefault(key, defaultValue);
  }

  @Override
  public void replaceAll(BiFunction<? super K, ? super Collection<V>, ? extends Collection<V>> function) {
    map.replaceAll(function);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    Collection<V> values = map.get(key);
    if (values != null && values.remove(oldValue)) {
      values.add(newValue);
      return true;
    }
    return false;
  }

  @Override
  public Collection<V> replace(K key, Collection<V> value) {
    return map.replace(key, value);
  }

}
