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

package io.atomix.core.treemap.impl;

import com.google.common.base.Throwables;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.core.treemap.AsyncAtomicTreeMap;
import io.atomix.core.treemap.AtomicTreeMap;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.Synchronous;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Implementation of the {@link AtomicTreeMap} interface.
 */
public class BlockingAtomicTreeMap<V>
    extends Synchronous<AsyncAtomicTreeMap<V>>
    implements AtomicTreeMap<V> {
  private final AsyncAtomicTreeMap<V> treeMap;
  private final long operationTimeoutMillis;

  public BlockingAtomicTreeMap(AsyncAtomicTreeMap<V> treeMap, long operationTimeoutMillis) {
    super(treeMap);
    this.treeMap = treeMap;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  private <T> T complete(CompletableFuture<T> future) {
    try {
      return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause());
      throw new PrimitiveException(e.getCause());
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
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
  public Map<String, Versioned<V>> getAllPresent(Iterable<String> keys) {
    return complete(treeMap.getAllPresent(keys));
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
  public DistributedSet<String> keySet() {
    return new BlockingDistributedSet<>(treeMap.keySet(), operationTimeoutMillis);
  }

  @Override
  public DistributedCollection<Versioned<V>> values() {
    return new BlockingDistributedCollection<>(treeMap.values(), operationTimeoutMillis);
  }

  @Override
  public DistributedSet<Map.Entry<String, Versioned<V>>> entrySet() {
    return new BlockingDistributedSet<>(treeMap.entrySet(), operationTimeoutMillis);
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
  public void addListener(AtomicMapEventListener<String, V> listener, Executor executor) {
    complete(treeMap.addListener(listener, executor));
  }

  @Override
  public void removeListener(AtomicMapEventListener<String, V> listener) {
    complete(treeMap.removeListener(listener));
  }

  @Override
  public NavigableMap<String, V> subMap(String upperKey, String lowerKey, boolean inclusiveUpper, boolean inclusiveLower) {
    return complete(treeMap.subMap(upperKey, lowerKey,
        inclusiveUpper, inclusiveLower));
  }

  @Override
  public AsyncAtomicTreeMap<V> async() {
    return treeMap;
  }
}
