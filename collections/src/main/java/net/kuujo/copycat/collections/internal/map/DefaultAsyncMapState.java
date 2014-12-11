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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Default asynchronous map state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncMapState<K, V> implements AsyncMapState<K, V> {

  /**
   * Gets the state map from the map state context.
   */
  private Map<K, V> getMap(StateContext<AsyncMapState<K, V>> context) {
    Map<K, V> map = context.get("value");
    if (map == null) {
      map = new HashMap<>();
      context.put("value", map);
    }
    return map;
  }

  @Override
  public V put(K key, V value, StateContext<AsyncMapState<K, V>> context) {
    return getMap(context).put(key, value);
  }

  @Override
  public V get(K key, StateContext<AsyncMapState<K, V>> context) {
    return getMap(context).get(key);
  }

  @Override
  public V remove(K key, StateContext<AsyncMapState<K, V>> context) {
    return getMap(context).remove(key);
  }

  @Override
  public boolean containsKey(K key, StateContext<AsyncMapState<K, V>> context) {
    return getMap(context).containsKey(key);
  }

  @Override
  public Set<K> keySet(StateContext<AsyncMapState<K, V>> context) {
    return getMap(context).keySet();
  }

  @Override
  public Set<Map.Entry<K, V>> entrySet(StateContext<AsyncMapState<K, V>> context) {
    return getMap(context).entrySet();
  }

  @Override
  public Collection<V> values(StateContext<AsyncMapState<K, V>> context) {
    return getMap(context).values();
  }

  @Override
  public int size(StateContext<AsyncMapState<K, V>> context) {
    return getMap(context).size();
  }

  @Override
  public boolean isEmpty(StateContext<AsyncMapState<K, V>> context) {
    return getMap(context).isEmpty();
  }

  @Override
  public void clear(StateContext<AsyncMapState<K, V>> context) {
    getMap(context).clear();
  }

}
