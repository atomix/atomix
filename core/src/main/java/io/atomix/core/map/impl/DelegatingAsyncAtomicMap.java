// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.core.map.impl;

import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Predicate;

import io.atomix.core.collection.AsyncDistributedCollection;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.set.AsyncDistributedSet;
import io.atomix.core.transaction.TransactionId;
import io.atomix.core.transaction.TransactionLog;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.impl.DelegatingAsyncPrimitive;
import io.atomix.utils.time.Versioned;

/**
 * {@code AsyncConsistentMap} that merely delegates control to
 * another AsyncConsistentMap.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class DelegatingAsyncAtomicMap<K, V>
    extends DelegatingAsyncPrimitive<AsyncAtomicMap<K, V>> implements AsyncAtomicMap<K, V> {

  DelegatingAsyncAtomicMap(AsyncAtomicMap<K, V> delegate) {
    super(delegate);
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
  public CompletableFuture<Versioned<V>> computeIf(K key,
                                                   Predicate<? super V> condition,
                                                   BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
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
  public AsyncDistributedSet<Entry<K, Versioned<V>>> entrySet() {
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
  public CompletableFuture<Long> lock(K key) {
    return delegate().lock(key);
  }

  @Override
  public CompletableFuture<OptionalLong> tryLock(K key) {
    return delegate().tryLock(key);
  }

  @Override
  public CompletableFuture<OptionalLong> tryLock(K key, Duration timeout) {
    return delegate().tryLock(key, timeout);
  }

  @Override
  public CompletableFuture<Boolean> isLocked(K key) {
    return delegate().isLocked(key);
  }

  @Override
  public CompletableFuture<Boolean> isLocked(K key, long version) {
    return delegate().isLocked(key, version);
  }

  @Override
  public CompletableFuture<Void> unlock(K key) {
    return delegate().unlock(key);
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
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    delegate().addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    delegate().removeStateChangeListener(listener);
  }

  @Override
  public AtomicMap<K, V> sync(Duration operationTimeout) {
    return new BlockingAtomicMap<>(this, operationTimeout.toMillis());
  }
}
