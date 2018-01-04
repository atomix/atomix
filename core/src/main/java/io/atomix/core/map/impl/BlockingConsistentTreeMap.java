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

package io.atomix.core.map.impl;

import com.google.common.base.Throwables;
import io.atomix.core.map.AsyncConsistentTreeMap;
import io.atomix.core.map.ConsistentMapBackedJavaMap;
import io.atomix.core.map.ConsistentMapException;
import io.atomix.core.map.ConsistentTreeMap;
import io.atomix.core.map.MapEventListener;
import io.atomix.primitive.Synchronous;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
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
public class BlockingConsistentTreeMap<V>
    extends Synchronous<AsyncConsistentTreeMap<V>>
    implements ConsistentTreeMap<V> {
  private final AsyncConsistentTreeMap<V> treeMap;
  private final long operationTimeoutMillis;
  private Map<String, V> javaMap;

  public BlockingConsistentTreeMap(AsyncConsistentTreeMap<V> treeMap, long operationTimeoutMillis) {
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
  public String firstKey() {
    return complete(treeMap.firstKey());
  }

  @Override
  public String lastKey() {
    return complete(treeMap.lastKey());
  }

  @Override
  public Map.Entry<String, Versioned<V>> ceilingEntry(String key) {
    return complete(treeMap.ceilingEntry(key));
  }

  @Override
  public Map.Entry<String, Versioned<V>> floorEntry(String key) {
    return complete(treeMap.floorEntry(key));
  }

  @Override
  public Map.Entry<String, Versioned<V>> higherEntry(String key) {
    return complete(treeMap.higherEntry(key));
  }

  @Override
  public Map.Entry<String, Versioned<V>> lowerEntry(String key) {
    return complete(treeMap.lowerEntry(key));
  }

  @Override
  public Map.Entry<String, Versioned<V>> firstEntry() {
    return complete(treeMap.firstEntry());
  }

  @Override
  public Map.Entry<String, Versioned<V>> lastEntry() {
    return complete(treeMap.lastEntry());
  }

  @Override
  public Map.Entry<String, Versioned<V>> pollFirstEntry() {
    return complete(treeMap.pollFirstEntry());
  }

  @Override
  public Map.Entry<String, Versioned<V>> pollLastEntry() {
    return complete(treeMap.pollLastEntry());
  }

  @Override
  public String lowerKey(String key) {
    return complete(treeMap.lowerKey(key));
  }

  @Override
  public String floorKey(String key) {
    return complete(treeMap.floorKey(key));
  }

  @Override
  public String ceilingKey(String key) {
    return complete(treeMap.ceilingKey(key));
  }

  @Override
  public String higherKey(String key) {
    return complete(treeMap.higherKey(key));
  }

  @Override
  public NavigableSet<String> navigableKeySet() {
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
  public boolean containsKey(String key) {
    return complete(treeMap.containsKey(key));
  }

  @Override
  public boolean containsValue(V value) {
    return complete(treeMap.containsValue(value));
  }

  @Override
  public Versioned<V> get(String key) {
    return complete(treeMap.get(key));
  }

  @Override
  public Versioned<V> getOrDefault(String key, V defaultValue) {
    return complete(treeMap.getOrDefault(key, defaultValue));
  }

  @Override
  public Versioned<V> computeIfAbsent(String key, Function<? super String, ? extends V> mappingFunction) {
    return complete(treeMap.computeIfAbsent(key, mappingFunction));
  }

  @Override
  public Versioned<V> compute(String key, BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
    return complete(treeMap.compute(key, remappingFunction));
  }

  @Override
  public Versioned<V> computeIfPresent(String key, BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
    return complete(treeMap.computeIfPresent(key, remappingFunction));
  }

  @Override
  public Versioned<V> computeIf(String key,
                                Predicate<? super V> condition,
                                BiFunction<? super String, ? super V, ? extends V> remappingFunction) {
    return complete(treeMap.computeIf(key, condition, remappingFunction));
  }

  @Override
  public Versioned<V> put(String key, V value, Duration ttl) {
    return complete(treeMap.put(key, value, ttl));
  }

  @Override
  public Versioned<V> putAndGet(String key, V value, Duration ttl) {
    return complete(treeMap.putAndGet(key, value, ttl));
  }

  @Override
  public Versioned<V> remove(String key) {
    return complete(treeMap.remove(key));
  }

  @Override
  public void clear() {
    complete(treeMap.clear());
  }

  @Override
  public Set<String> keySet() {
    return complete(treeMap.keySet());
  }

  @Override
  public Collection<Versioned<V>> values() {
    return complete(treeMap.values());
  }

  @Override
  public Set<Map.Entry<String, Versioned<V>>> entrySet() {
    return complete(treeMap.entrySet());
  }

  @Override
  public Versioned<V> putIfAbsent(String key, V value, Duration ttl) {
    return complete(treeMap.putIfAbsent(key, value, ttl));
  }

  @Override
  public boolean remove(String key, V value) {
    return complete(treeMap.remove(key, value));
  }

  @Override
  public boolean remove(String key, long version) {
    return complete(treeMap.remove(key, version));
  }

  @Override
  public Versioned<V> replace(String key, V value) {
    return complete(treeMap.replace(key, value));
  }

  @Override
  public boolean replace(String key, V oldValue, V newValue) {
    return complete(treeMap.replace(key, oldValue, newValue));
  }

  @Override
  public boolean replace(String key, long oldVersion, V newValue) {
    return complete(treeMap.replace(key, oldVersion, newValue));
  }

  @Override
  public void addListener(MapEventListener<String, V> listener, Executor executor) {
    complete(treeMap.addListener(listener, executor));
  }

  @Override
  public void removeListener(MapEventListener<String, V> listener) {
    complete(treeMap.removeListener(listener));
  }

  @Override
  public Map<String, V> asJavaMap() {
    synchronized (this) {
      if (javaMap == null) {
        javaMap = new ConsistentMapBackedJavaMap<>(this);
      }
    }
    return javaMap;
  }

  @Override
  public NavigableMap<String, V> subMap(String upperKey, String lowerKey, boolean inclusiveUpper, boolean inclusiveLower) {
    return complete(treeMap.subMap(upperKey, lowerKey,
        inclusiveUpper, inclusiveLower));
  }

  @Override
  public AsyncConsistentTreeMap<V> async() {
    return treeMap;
  }
}
