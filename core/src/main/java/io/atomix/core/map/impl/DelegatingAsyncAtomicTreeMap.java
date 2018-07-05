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

import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.core.map.AsyncAtomicTreeMap;
import io.atomix.core.map.AtomicTreeMap;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;
import io.atomix.primitive.protocol.PrimitiveProtocol;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A {@link AsyncAtomicTreeMap} that delegates control to another instance
 * of {@link AsyncAtomicTreeMap}.
 */
public class DelegatingAsyncAtomicTreeMap<K extends Comparable<K>, V>
    extends DelegatingAsyncPrimitive
    implements AsyncAtomicTreeMap<K, V> {

  private final AsyncAtomicTreeMap<K, V> delegateMap;

  DelegatingAsyncAtomicTreeMap(AsyncAtomicTreeMap<K, V> delegateMap) {
    super(delegateMap);
    this.delegateMap = checkNotNull(delegateMap,
        "delegate map cannot be null");
  }

  @Override
  public PrimitiveProtocol protocol() {
    return delegateMap.protocol();
  }

  @Override
  public CompletableFuture<K> firstKey() {
    return delegateMap.firstKey();
  }

  @Override
  public CompletableFuture<K> lastKey() {
    return delegateMap.lastKey();
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> ceilingEntry(K key) {
    return delegateMap.ceilingEntry(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> floorEntry(K key) {
    return delegateMap.floorEntry(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> higherEntry(K key) {
    return delegateMap.higherEntry(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> lowerEntry(K key) {
    return delegateMap.lowerEntry(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> firstEntry() {
    return delegateMap.firstEntry();
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> lastEntry() {
    return delegateMap.lastEntry();
  }

  @Override
  public CompletableFuture<K> lowerKey(K key) {
    return delegateMap.lowerKey(key);
  }

  @Override
  public CompletableFuture<K> floorKey(K key) {
    return delegateMap.floorKey(key);
  }

  @Override
  public CompletableFuture<K> ceilingKey(K key) {
    return delegateMap.ceilingKey(key);
  }

  @Override
  public CompletableFuture<K> higherKey(K key) {
    return delegateMap.higherKey(key);
  }

  @Override
  public CompletableFuture<NavigableSet<K>> navigableKeySet() {
    return delegateMap.navigableKeySet();
  }

  @Override
  public CompletableFuture<NavigableMap<K, V>> subMap(
      K upperKey,
      K lowerKey,
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
  public CompletableFuture<Versioned<V>> computeIf(
      K key,
      Predicate<? super V> condition,
      BiFunction<? super K, ? super V,
          ? extends V> remappingFunction) {
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
  public AsyncDistributedSet<K> keySet() {
    return delegateMap.keySet();
  }

  @Override
  public AsyncDistributedCollection<Versioned<V>> values() {
    return delegateMap.values();
  }

  @Override
  public AsyncDistributedSet<Map.Entry<K, Versioned<V>>> entrySet() {
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
  public CompletableFuture<Boolean> replace(K key, V oldValue,
                                            V newValue) {
    return delegateMap.replace(key, oldValue, newValue);
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, long oldVersion,
                                            V newValue) {
    return delegateMap.replace(key, oldVersion, newValue);
  }

  @Override
  public CompletableFuture<Void> addListener(
      AtomicMapEventListener<K, V> listener, Executor executor) {
    return delegateMap.addListener(listener, executor);
  }

  @Override
  public CompletableFuture<Void> removeListener(
      AtomicMapEventListener<K, V> listener) {
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
  public AtomicTreeMap<K, V> sync(Duration operationTimeout) {
    return new BlockingAtomicTreeMap<>(this, operationTimeout.toMillis());
  }
}
