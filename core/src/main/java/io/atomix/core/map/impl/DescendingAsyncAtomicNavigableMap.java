/*
 * Copyright 2018-present Open Networking Foundation
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
package io.atomix.core.map.impl;

import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.map.AsyncAtomicNavigableMap;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.map.AtomicNavigableMap;
import io.atomix.core.set.AsyncDistributedNavigableSet;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * Asynchronous atomic navigable map that operates on the map in descending order.
 */
public class DescendingAsyncAtomicNavigableMap<K extends Comparable<K>, V>
    extends DelegatingAsyncPrimitive<AsyncAtomicNavigableMap<K, V>>
    implements AsyncAtomicNavigableMap<K, V> {

  public DescendingAsyncAtomicNavigableMap(AsyncAtomicNavigableMap<K, V> primitive) {
    super(primitive);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> lowerEntry(K key) {
    return delegate().higherEntry(key);
  }

  @Override
  public CompletableFuture<K> lowerKey(K key) {
    return delegate().higherKey(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> floorEntry(K key) {
    return delegate().ceilingEntry(key);
  }

  @Override
  public CompletableFuture<K> floorKey(K key) {
    return delegate().ceilingKey(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> ceilingEntry(K key) {
    return delegate().floorEntry(key);
  }

  @Override
  public CompletableFuture<K> ceilingKey(K key) {
    return delegate().floorKey(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> higherEntry(K key) {
    return delegate().lowerEntry(key);
  }

  @Override
  public CompletableFuture<K> higherKey(K key) {
    return delegate().lowerKey(key);
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> firstEntry() {
    return delegate().lastEntry();
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> lastEntry() {
    return delegate().firstEntry();
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> pollFirstEntry() {
    return delegate().pollLastEntry();
  }

  @Override
  public CompletableFuture<Map.Entry<K, Versioned<V>>> pollLastEntry() {
    return delegate().pollFirstEntry();
  }

  @Override
  public AsyncAtomicNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
    return delegate().subMap(fromKey, fromInclusive, toKey, toInclusive).descendingMap();
  }

  @Override
  public AsyncAtomicNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
    return delegate().headMap(toKey, inclusive).descendingMap();
  }

  @Override
  public AsyncAtomicNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
    return delegate().tailMap(fromKey, inclusive).descendingMap();
  }

  @Override
  public AsyncAtomicNavigableMap<K, V> descendingMap() {
    return delegate();
  }

  @Override
  public AsyncDistributedNavigableSet<K> navigableKeySet() {
    return delegate().navigableKeySet().descendingSet();
  }

  @Override
  public AsyncDistributedNavigableSet<K> descendingKeySet() {
    return delegate().navigableKeySet();
  }

  @Override
  public CompletableFuture<K> firstKey() {
    return delegate().lastKey();
  }

  @Override
  public CompletableFuture<K> lastKey() {
    return delegate().firstKey();
  }

  @Override
  public CompletableFuture<Integer> size() {
    return delegate().size();
  }

  @Override
  public CompletableFuture<Boolean> containsKey(K key) {
    return delegate().containsKey(key);
  }

  @Override
  public CompletableFuture<Boolean> containsValue(V value) {
    return delegate().containsValue(value);
  }

  @Override
  public CompletableFuture<Versioned<V>> get(K key) {
    return delegate().get(key);
  }

  @Override
  public CompletableFuture<Map<K, Versioned<V>>> getAllPresent(Iterable<K> keys) {
    return delegate().getAllPresent(keys);
  }

  @Override
  public CompletableFuture<Versioned<V>> getOrDefault(K key, V defaultValue) {
    return delegate().getOrDefault(key, defaultValue);
  }

  @Override
  public CompletableFuture<Versioned<V>> computeIf(K key, Predicate<? super V> condition, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return delegate().computeIf(key, condition, remappingFunction);
  }

  @Override
  public CompletableFuture<Versioned<V>> put(K key, V value, Duration ttl) {
    return delegate().put(key, value, ttl);
  }

  @Override
  public CompletableFuture<Versioned<V>> putAndGet(K key, V value, Duration ttl) {
    return delegate().putAndGet(key, value, ttl);
  }

  @Override
  public CompletableFuture<Versioned<V>> remove(K key) {
    return delegate().remove(key);
  }

  @Override
  public CompletableFuture<Void> clear() {
    return delegate().clear();
  }

  @Override
  public AsyncDistributedSet<K> keySet() {
    return delegate().keySet();
  }

  @Override
  public AsyncDistributedCollection<Versioned<V>> values() {
    return delegate().values();
  }

  @Override
  public AsyncDistributedSet<Map.Entry<K, Versioned<V>>> entrySet() {
    return delegate().entrySet();
  }

  @Override
  public CompletableFuture<Versioned<V>> putIfAbsent(K key, V value, Duration ttl) {
    return delegate().putIfAbsent(key, value, ttl);
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return delegate().remove(key, value);
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, long version) {
    return delegate().remove(key, version);
  }

  @Override
  public CompletableFuture<Versioned<V>> replace(K key, V value) {
    return delegate().replace(key, value);
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return delegate().replace(key, oldValue, newValue);
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, long oldVersion, V newValue) {
    return delegate().replace(key, oldVersion, newValue);
  }

  @Override
  public CompletableFuture<Void> addListener(AtomicMapEventListener<K, V> listener, Executor executor) {
    return delegate().addListener(listener, executor);
  }

  @Override
  public CompletableFuture<Void> removeListener(AtomicMapEventListener<K, V> listener) {
    return delegate().removeListener(listener);
  }

  @Override
  public CompletableFuture<Boolean> prepare(TransactionLog<MapUpdate<K, V>> transactionLog) {
    return delegate().prepare(transactionLog);
  }

  @Override
  public CompletableFuture<Void> commit(TransactionId transactionId) {
    return delegate().commit(transactionId);
  }

  @Override
  public CompletableFuture<Void> rollback(TransactionId transactionId) {
    return delegate().rollback(transactionId);
  }

  @Override
  public AtomicNavigableMap<K, V> sync(Duration operationTimeout) {
    return new BlockingAtomicNavigableMap<>(this, operationTimeout.toMillis());
  }
}
