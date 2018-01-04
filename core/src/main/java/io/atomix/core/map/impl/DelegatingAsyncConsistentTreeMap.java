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

import io.atomix.core.map.AsyncConsistentTreeMap;
import io.atomix.core.map.ConsistentTreeMap;
import io.atomix.core.map.MapEventListener;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.impl.DelegatingDistributedPrimitive;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link AsyncConsistentTreeMap} that delegates control to another instance
 * of {@link AsyncConsistentTreeMap}.
 */
public class DelegatingAsyncConsistentTreeMap<V>
    extends DelegatingDistributedPrimitive
    implements AsyncConsistentTreeMap<V> {

  private final AsyncConsistentTreeMap<V> delegateMap;

  DelegatingAsyncConsistentTreeMap(AsyncConsistentTreeMap<V> delegateMap) {
    super(delegateMap);
    this.delegateMap = checkNotNull(delegateMap,
        "delegate map cannot be null");
  }

  @Override
  public CompletableFuture<String> firstKey() {
    return delegateMap.firstKey();
  }

  @Override
  public CompletableFuture<String> lastKey() {
    return delegateMap.lastKey();
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<V>>> ceilingEntry(String key) {
    return delegateMap.ceilingEntry(key);
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<V>>> floorEntry(String key) {
    return delegateMap.floorEntry(key);
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<V>>> higherEntry(String key) {
    return delegateMap.higherEntry(key);
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<V>>> lowerEntry(String key) {
    return delegateMap.lowerEntry(key);
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<V>>> firstEntry() {
    return delegateMap.firstEntry();
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<V>>> lastEntry() {
    return delegateMap.lastEntry();
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<V>>> pollFirstEntry() {
    return delegateMap.pollFirstEntry();
  }

  @Override
  public CompletableFuture<Map.Entry<String, Versioned<V>>> pollLastEntry() {
    return delegateMap.pollLastEntry();
  }

  @Override
  public CompletableFuture<String> lowerKey(String key) {
    return delegateMap.lowerKey(key);
  }

  @Override
  public CompletableFuture<String> floorKey(String key) {
    return delegateMap.floorKey(key);
  }

  @Override
  public CompletableFuture<String> ceilingKey(String key) {
    return delegateMap.ceilingKey(key);
  }

  @Override
  public CompletableFuture<String> higherKey(String key) {
    return delegateMap.higherKey(key);
  }

  @Override
  public CompletableFuture<NavigableSet<String>> navigableKeySet() {
    return delegateMap.navigableKeySet();
  }

  @Override
  public CompletableFuture<NavigableMap<String, V>> subMap(
      String upperKey,
      String lowerKey,
      boolean inclusiveUpper,
      boolean inclusiveLower) {
    return delegateMap.subMap(upperKey, lowerKey,
        inclusiveUpper, inclusiveLower);
  }

  @Override
  public CompletableFuture<Integer> size() {
    return delegateMap.size();
  }

  @Override
  public CompletableFuture<Boolean> containsKey(String key) {
    return delegateMap.containsKey(key);
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    return delegateMap.containsValue(value);
  }

  @Override
  public CompletableFuture<Versioned<V>> get(String key) {
    return delegateMap.get(key);
  }

  @Override
  public CompletableFuture<Map<String, Versioned<V>>> getAllPresent(Iterable<String> keys) {
    return delegateMap.getAllPresent(keys);
  }

  @Override
  public CompletableFuture<Versioned<V>> getOrDefault(String key, V defaultValue) {
    return delegateMap.getOrDefault(key, defaultValue);
  }

  @Override
  public CompletableFuture<Versioned<V>> computeIf(
      String key,
      Predicate<? super V> condition,
      BiFunction<? super String, ? super V,
          ? extends V> remappingFunction) {
    return delegateMap.computeIf(key, condition, remappingFunction);
  }

  @Override
  public CompletableFuture<Versioned<V>> put(String key, V value, Duration ttl) {
    return delegateMap.put(key, value, ttl);
  }

  @Override
  public CompletableFuture<Versioned<V>> putAndGet(String key, V value, Duration ttl) {
    return delegateMap.putAndGet(key, value, ttl);
  }

  @Override
  public CompletableFuture<Versioned<V>> remove(String key) {
    return delegateMap.remove(key);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return delegateMap.clear();
  }

  @Override
  public CompletableFuture<Set<String>> keySet() {
    return delegateMap.keySet();
  }

  @Override
  public CompletableFuture<Collection<Versioned<V>>> values() {
    return delegateMap.values();
  }

  @Override
  public CompletableFuture<Set<Map.Entry<String, Versioned<V>>>> entrySet() {
    return delegateMap.entrySet();
  }

  @Override
  public CompletableFuture<Versioned<V>> putIfAbsent(String key, V value, Duration ttl) {
    return delegateMap.putIfAbsent(key, value, ttl);
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, V value) {
    return delegateMap.remove(key, value);
  }

  @Override
  public CompletableFuture<Boolean> remove(String key, long version) {
    return delegateMap.remove(key, version);
  }

  @Override
  public CompletableFuture<Versioned<V>> replace(String key, V value) {
    return delegateMap.replace(key, value);
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, V oldValue,
                                            V newValue) {
    return delegateMap.replace(key, oldValue, newValue);
  }

  @Override
  public CompletableFuture<Boolean> replace(String key, long oldVersion,
                                            V newValue) {
    return delegateMap.replace(key, oldVersion, newValue);
  }

  @Override
  public CompletableFuture<Void> addListener(
      MapEventListener<String, V> listener, Executor executor) {
    return delegateMap.addListener(listener, executor);
  }

  @Override
  public CompletableFuture<Void> removeListener(
      MapEventListener<String, V> listener) {
    return delegateMap.removeListener(listener);
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<String, V>> transactionLog) {
    return delegateMap.prepare(transactionLog);
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return delegateMap.commit(transactionId);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return delegateMap.rollback(transactionId);
  }

  @Override
  public ConsistentTreeMap<V> sync(Duration operationTimeout) {
    return new BlockingConsistentTreeMap<>(this, operationTimeout.toMillis());
  }
}
