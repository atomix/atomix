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
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.BiFunction;

/**
 * Default asynchronous multimap state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultMultiMapState<K, V> implements MultiMapState<K, V> {
  private Map<K, Collection<V>> map;

  @Override
  public void init(StateContext<MultiMapState<K, V>> context) {
    map = context.get("value");
    if (map == null) {
      map = new HashMap<>();
      context.put("value", map);
    }
  }

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

  @NotNull
  @Override
  public Set<K> keySet() {
    return map.keySet();
  }

  @NotNull
  @Override
  public Collection<Collection<V>> values() {
    return map.values();
  }

  @NotNull
  @Override
  public Set<Map.Entry<K, Collection<V>>> entrySet() {
    return map.entrySet();
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
