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

import java.util.*;

/**
 * Default asynchronous multimap state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DefaultAsyncMultiMapState<K, V> implements AsyncMultiMapState<K, V> {

  /**
   * Gets the state map from the map state context.
   */
  private Map<K, Collection<V>> getMultiMap(StateContext<AsyncMultiMapState<K, V>> context) {
    Map<K, Collection<V>> map = context.get("value");
    if (map == null) {
      map = new HashMap<>();
      context.put("value", map);
    }
    return map;
  }

  /**
   * Gets a collection from the map state context.
   */
  private Collection<V> getCollection(K key, StateContext<AsyncMultiMapState<K, V>> context) {
    Map<K, Collection<V>> map = getMultiMap(context);
    Collection<V> collection = map.get(key);
    if (collection == null) {
      collection = new HashSet<>();
      map.put(key, collection);
    }
    return collection;
  }

  @Override
  public boolean put(K key, V value, StateContext<AsyncMultiMapState<K, V>> context) {
    return getCollection(key, context).add(value);
  }

  @Override
  public Collection<V> get(K key, StateContext<AsyncMultiMapState<K, V>> context) {
    return getCollection(key, context);
  }

  @Override
  public Collection<V> remove(K key, StateContext<AsyncMultiMapState<K, V>> context) {
    return getMultiMap(context).remove(key);
  }

  @Override
  public boolean remove(K key, V value, StateContext<AsyncMultiMapState<K, V>> context) {
    return getCollection(key, context).remove(value);
  }

  @Override
  public boolean containsKey(K key, StateContext<AsyncMultiMapState<K, V>> context) {
    return getMultiMap(context).containsKey(key);
  }

  @Override
  public boolean containsValue(V value, StateContext<AsyncMultiMapState<K, V>> context) {
    for (Collection<V> collection : getMultiMap(context).values()) {
      if (collection.contains(value)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean containsEntry(K key, V value, StateContext<AsyncMultiMapState<K, V>> context) {
    return getCollection(key, context).contains(value);
  }

  @Override
  public Set<K> keySet(StateContext<AsyncMultiMapState<K, V>> context) {
    return getMultiMap(context).keySet();
  }

  @Override
  public Set<Map.Entry<K, Collection<V>>> entrySet(StateContext<AsyncMultiMapState<K, V>> context) {
    return getMultiMap(context).entrySet();
  }

  @Override
  public Collection<V> values(StateContext<AsyncMultiMapState<K, V>> context) {
    Collection<V> collection = new ArrayList<>();
    for (Collection<V> c : getMultiMap(context).values()) {
      collection.addAll(c);
    }
    return collection;
  }

  @Override
  public int size(StateContext<AsyncMultiMapState<K, V>> context) {
    return getMultiMap(context).size();
  }

  @Override
  public boolean isEmpty(StateContext<AsyncMultiMapState<K, V>> context) {
    return getMultiMap(context).isEmpty();
  }

  @Override
  public void clear(StateContext<AsyncMultiMapState<K, V>> context) {
    getMultiMap(context).clear();
  }

}
