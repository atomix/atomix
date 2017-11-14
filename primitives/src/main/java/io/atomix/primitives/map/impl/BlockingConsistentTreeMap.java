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

package io.atomix.primitives.map.impl;

import com.google.common.base.Throwables;
import io.atomix.primitives.Synchronous;
import io.atomix.primitives.map.AsyncConsistentTreeMap;
import io.atomix.primitives.map.ConsistentMapBackedJavaMap;
import io.atomix.primitives.map.ConsistentMapException;
import io.atomix.primitives.map.ConsistentTreeMap;
import io.atomix.primitives.map.MapEventListener;
import io.atomix.time.Versioned;

import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Implementation of the {@link ConsistentTreeMap} interface.
 */
public class BlockingConsistentTreeMap<K, V>
    extends Synchronous<AsyncConsistentTreeMap<K, V>>
    implements ConsistentTreeMap<K, V> {
  private final AsyncConsistentTreeMap<K, V> treeMap;
  private final long operationTimeoutMillis;
  private Map<K, V> javaMap;

  public BlockingConsistentTreeMap(AsyncConsistentTreeMap<K, V> treeMap, long operationTimeoutMillis) {
    super(treeMap);
    this.treeMap = treeMap;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  private <T> T complete(CompletableFuture<T> future) {
    try {
      return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConsistentMapException.Interrupted();
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause());
      throw new ConsistentMapException(e.getCause());
    } catch (TimeoutException e) {
      throw new ConsistentMapException.Timeout();
    }
  }

  @Override
  public K firstKey() {
    return complete(treeMap.firstKey());
  }

  @Override
  public K lastKey() {
    return complete(treeMap.lastKey());
  }

  @Override
  public Map.Entry<K, Versioned<V>> ceilingEntry(K key) {
    return complete(treeMap.ceilingEntry(key));
  }

  @Override
  public Map.Entry<K, Versioned<V>> floorEntry(K key) {
    return complete(treeMap.floorEntry(key));
  }

  @Override
  public Map.Entry<K, Versioned<V>> higherEntry(K key) {
    return complete(treeMap.higherEntry(key));
  }

  @Override
  public Map.Entry<K, Versioned<V>> lowerEntry(K key) {
    return complete(treeMap.lowerEntry(key));
  }

  @Override
  public Map.Entry<K, Versioned<V>> firstEntry() {
    return complete(treeMap.firstEntry());
  }

  @Override
  public Map.Entry<K, Versioned<V>> lastEntry() {
    return complete(treeMap.lastEntry());
  }

  @Override
  public Map.Entry<K, Versioned<V>> pollFirstEntry() {
    return complete(treeMap.pollFirstEntry());
  }

  @Override
  public Map.Entry<K, Versioned<V>> pollLastEntry() {
    return complete(treeMap.pollLastEntry());
  }

  @Override
  public K lowerKey(K key) {
    return complete(treeMap.lowerKey(key));
  }

  @Override
  public K floorKey(K key) {
    return complete(treeMap.floorKey(key));
  }

  @Override
  public K ceilingKey(K key) {
    return complete(treeMap.ceilingKey(key));
  }

  @Override
  public K higherKey(K key) {
    return complete(treeMap.higherKey(key));
  }

  @Override
  public NavigableSet<K> navigableKeySet() {
    return complete(treeMap.navigableKeySet());
  }

  @Override
  public int size() {
    return complete(treeMap.size());
  }

  @Override
  public boolean isEmpty() {
    return complete(treeMap.isEmpty());
  }

  @Override
  public boolean containsKey(K key) {
    return complete(treeMap.containsKey(key));
  }

  @Override
  public boolean containsValue(V value) {
    return complete(treeMap.containsValue(value));
  }

  @Override
  public Versioned<V> get(K key) {
    return complete(treeMap.get(key));
  }

  @Override
  public Versioned<V> getOrDefault(K key, V defaultValue) {
    return complete(treeMap.getOrDefault(key, defaultValue));
  }

  @Override
  public Versioned<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
    return complete(treeMap.computeIfAbsent(key, mappingFunction));
  }

  @Override
  public Versioned<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return complete(treeMap.compute(key, remappingFunction));
  }

  @Override
  public Versioned<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return complete(treeMap.computeIfPresent(key, remappingFunction));
  }

  @Override
  public Versioned<V> computeIf(K key,
                                Predicate<? super V> condition,
                                BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return complete(treeMap.computeIf(key, condition, remappingFunction));
  }

  @Override
  public Versioned<V> put(K key, V value) {
    return complete(treeMap.put(key, value));
  }

  @Override
  public Versioned<V> putAndGet(K key, V value) {
    return complete(treeMap.putAndGet(key, value));
  }

  @Override
  public Versioned<V> remove(K key) {
    return complete(treeMap.remove(key));
  }

  @Override
  public void clear() {
    complete(treeMap.clear());
  }

  @Override
  public Set<K> keySet() {
    return complete(treeMap.keySet());
  }

  @Override
  public Collection<Versioned<V>> values() {
    return complete(treeMap.values());
  }

  @Override
  public Set<Map.Entry<K, Versioned<V>>> entrySet() {
    return complete(treeMap.entrySet());
  }

  @Override
  public Versioned<V> putIfAbsent(K key, V value) {
    return complete(treeMap.putIfAbsent(key, value));
  }

  @Override
  public boolean remove(K key, V value) {
    return complete(treeMap.remove(key, value));
  }

  @Override
  public boolean remove(K key, long version) {
    return complete(treeMap.remove(key, version));
  }

  @Override
  public Versioned<V> replace(K key, V value) {
    return complete(treeMap.replace(key, value));
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return complete(treeMap.replace(key, oldValue, newValue));
  }

  @Override
  public boolean replace(K key, long oldVersion, V newValue) {
    return complete(treeMap.replace(key, oldVersion, newValue));
  }

  @Override
  public void addListener(MapEventListener<K, V> listener, Executor executor) {
    complete(treeMap.addListener(listener, executor));
  }

  @Override
  public void removeListener(MapEventListener<K, V> listener) {
    complete(treeMap.removeListener(listener));
  }

  @Override
  public Map<K, V> asJavaMap() {
    synchronized (this) {
      if (javaMap == null) {
        javaMap = new ConsistentMapBackedJavaMap<>(this);
      }
    }
    return javaMap;
  }

  @Override
  public NavigableMap<K, V> subMap(K upperKey, K lowerKey, boolean inclusiveUpper, boolean inclusiveLower) {
    return complete(treeMap.subMap(upperKey, lowerKey,
        inclusiveUpper, inclusiveLower));
  }
}
