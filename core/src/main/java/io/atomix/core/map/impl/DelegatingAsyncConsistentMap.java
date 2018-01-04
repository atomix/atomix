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

import com.google.common.base.MoreObjects;

import io.atomix.core.map.AsyncConsistentMap;
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.MapEventListener;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.impl.DelegatingDistributedPrimitive;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * {@code AsyncConsistentMap} that merely delegates control to
 * another AsyncConsistentMap.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class DelegatingAsyncConsistentMap<K, V>
    extends DelegatingDistributedPrimitive implements AsyncConsistentMap<K, V> {

  private final AsyncConsistentMap<K, V> delegateMap;

  DelegatingAsyncConsistentMap(AsyncConsistentMap<K, V> delegateMap) {
    super(delegateMap);
    this.delegateMap = delegateMap;
  }

  @Override
  public CompletableFuture<Integer> size() {
    return delegateMap.size();
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return delegateMap.containsKey(key);
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    return delegateMap.containsValue(value);
  }

  @Override
  public CompletableFuture<Versioned<V>> get(K key) {
    return delegateMap.get(key);
  }

  @Override
  public CompletableFuture<Map<K, Versioned<V>>> getAllPresent(Iterable<K> keys) {
    return delegateMap.getAllPresent(keys);
  }

  @Override
  public CompletableFuture<Versioned<V>> getOrDefault(K key, V defaultValue) {
    return delegateMap.getOrDefault(key, defaultValue);
  }

  @Override
  public CompletableFuture<Versioned<V>> computeIf(K key,
                                                   Predicate<? super V> condition,
                                                   BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return delegateMap.computeIf(key, condition, remappingFunction);
  }

  @Override
  public CompletableFuture<Versioned<V>> put(K key, V value, Duration ttl) {
    return delegateMap.put(key, value, ttl);
  }

  @Override
  public CompletableFuture<Versioned<V>> putAndGet(K key, V value, Duration ttl) {
    return delegateMap.putAndGet(key, value, ttl);
  }

  @Override
  public CompletableFuture<Versioned<V>> remove(K key) {
    return delegateMap.remove(key);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return delegateMap.clear();
  }

  @Override
  public CompletableFuture<Set<K>> keySet() {
    return delegateMap.keySet();
  }

  @Override
  public CompletableFuture<Collection<Versioned<V>>> values() {
    return delegateMap.values();
  }

  @Override
  public CompletableFuture<Set<Entry<K, Versioned<V>>>> entrySet() {
    return delegateMap.entrySet();
  }

  @Override
  public CompletableFuture<Versioned<V>> putIfAbsent(K key, V value, Duration ttl) {
    return delegateMap.putIfAbsent(key, value, ttl);
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return delegateMap.remove(key, value);
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, long version) {
    return delegateMap.remove(key, version);
  }

  @Override
  public CompletableFuture<Versioned<V>> replace(K key, V value) {
    return delegateMap.replace(key, value);
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return delegateMap.replace(key, oldValue, newValue);
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, long oldVersion, V newValue) {
    return delegateMap.replace(key, oldVersion, newValue);
  }

  @Override
  public CompletableFuture<Void> addListener(MapEventListener<K, V> listener, Executor executor) {
    return delegateMap.addListener(listener, executor);
  }

  @Override
  public CompletableFuture<Void> removeListener(MapEventListener<K, V> listener) {
    return delegateMap.removeListener(listener);
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<K, V>> transactionLog) {
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
  public void addStatusChangeListener(Consumer<Status> listener) {
    delegateMap.addStatusChangeListener(listener);
  }

  @Override
  public void removeStatusChangeListener(Consumer<Status> listener) {
    delegateMap.removeStatusChangeListener(listener);
  }

  @Override
  public Collection<Consumer<Status>> statusChangeListeners() {
    return delegateMap.statusChangeListeners();
  }

  @Override
  public ConsistentMap<K, V> sync(Duration operationTimeout) {
    return new BlockingConsistentMap<>(this, operationTimeout.toMillis());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(getClass())
        .add("delegateMap", delegateMap)
        .toString();
  }
}
