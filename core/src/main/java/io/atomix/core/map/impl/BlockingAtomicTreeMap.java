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
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.core.map.AsyncAtomicTreeMap;
import io.atomix.core.map.AtomicTreeMap;
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
public class BlockingAtomicTreeMap<K extends Comparable<K>, V>
    extends Synchronous<AsyncAtomicTreeMap<K, V>>
    implements AtomicTreeMap<K, V> {
  private final AsyncAtomicTreeMap<K, V> treeMap;
  private final long operationTimeoutMillis;

  public BlockingAtomicTreeMap(AsyncAtomicTreeMap<K, V> treeMap, long operationTimeoutMillis) {
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
  public Map<K, Versioned<V>> getAllPresent(Iterable<K> keys) {
    return complete(treeMap.getAllPresent(keys));
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
  public Versioned<V> put(K key, V value, Duration ttl) {
    return complete(treeMap.put(key, value, ttl));
  }

  @Override
  public Versioned<V> putAndGet(K key, V value, Duration ttl) {
    return complete(treeMap.putAndGet(key, value, ttl));
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
  public DistributedSet<K> keySet() {
    return new BlockingDistributedSet<>(treeMap.keySet(), operationTimeoutMillis);
  }

  @Override
  public DistributedCollection<Versioned<V>> values() {
    return new BlockingDistributedCollection<>(treeMap.values(), operationTimeoutMillis);
  }

  @Override
  public DistributedSet<Map.Entry<K, Versioned<V>>> entrySet() {
    return new BlockingDistributedSet<>(treeMap.entrySet(), operationTimeoutMillis);
  }

  @Override
  public Versioned<V> putIfAbsent(K key, V value, Duration ttl) {
    return complete(treeMap.putIfAbsent(key, value, ttl));
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
  public void addListener(AtomicMapEventListener<K, V> listener, Executor executor) {
    complete(treeMap.addListener(listener, executor));
  }

  @Override
  public void removeListener(AtomicMapEventListener<K, V> listener) {
    complete(treeMap.removeListener(listener));
  }

  @Override
  public NavigableMap<K, V> subMap(K upperKey, K lowerKey, boolean inclusiveUpper, boolean inclusiveLower) {
    return complete(treeMap.subMap(upperKey, lowerKey,
        inclusiveUpper, inclusiveLower));
  }

  @Override
  public AsyncAtomicTreeMap<K, V> async() {
    return treeMap;
  }
}
