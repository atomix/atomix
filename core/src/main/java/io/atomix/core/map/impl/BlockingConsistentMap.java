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
import io.atomix.core.map.AsyncConsistentMap;
import io.atomix.core.map.ConsistentMap;
import io.atomix.core.map.ConsistentMapBackedJavaMap;
import io.atomix.core.map.ConsistentMapException;
import io.atomix.core.map.MapEventListener;
import io.atomix.primitive.Synchronous;
import io.atomix.utils.concurrent.Retries;
import io.atomix.utils.time.Versioned;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Default implementation of {@code ConsistentMap}.
 *
 * @param <K> type of key.
 * @param <V> type of value.
 */
public class BlockingConsistentMap<K, V> extends Synchronous<AsyncConsistentMap<K, V>> implements ConsistentMap<K, V> {

  private static final int MAX_DELAY_BETWEEN_RETRY_MILLS = 50;
  private final AsyncConsistentMap<K, V> asyncMap;
  private final long operationTimeoutMillis;
  private Map<K, V> javaMap;

  public BlockingConsistentMap(AsyncConsistentMap<K, V> asyncMap, long operationTimeoutMillis) {
    super(asyncMap);
    this.asyncMap = asyncMap;
    this.operationTimeoutMillis = operationTimeoutMillis;
  }

  @Override
  public int size() {
    return complete(asyncMap.size());
  }

  @Override
  public boolean isEmpty() {
    return complete(asyncMap.isEmpty());
  }

  @Override
  public boolean containsKey(K key) {
    return complete(asyncMap.containsKey(key));
  }

  @Override
  public boolean containsValue(V value) {
    return complete(asyncMap.containsValue(value));
  }

  @Override
  public Versioned<V> get(K key) {
    return complete(asyncMap.get(key));
  }

  @Override
  public Versioned<V> getOrDefault(K key, V defaultValue) {
    return complete(asyncMap.getOrDefault(key, defaultValue));
  }

  @Override
  public Versioned<V> computeIfAbsent(K key,
                                      Function<? super K, ? extends V> mappingFunction) {
    return computeIf(key, Objects::isNull, (k, v) -> mappingFunction.apply(k));
  }

  @Override
  public Versioned<V> computeIfPresent(K key,
                                       BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return computeIf(key, Objects::nonNull, remappingFunction);
  }

  @Override
  public Versioned<V> compute(K key,
                              BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return computeIf(key, v -> true, remappingFunction);
  }

  @Override
  public Versioned<V> computeIf(K key,
                                Predicate<? super V> condition,
                                BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return Retries.retryable(() -> complete(asyncMap.computeIf(key, condition, remappingFunction)),
        ConsistentMapException.ConcurrentModification.class,
        Integer.MAX_VALUE,
        MAX_DELAY_BETWEEN_RETRY_MILLS).get();
  }

  @Override
  public Versioned<V> put(K key, V value, Duration ttl) {
    return complete(asyncMap.put(key, value, ttl));
  }

  @Override
  public Versioned<V> putAndGet(K key, V value, Duration ttl) {
    return complete(asyncMap.putAndGet(key, value, ttl));
  }

  @Override
  public Versioned<V> remove(K key) {
    return complete(asyncMap.remove(key));
  }

  @Override
  public void clear() {
    complete(asyncMap.clear());
  }

  @Override
  public Set<K> keySet() {
    return complete(asyncMap.keySet());
  }

  @Override
  public Collection<Versioned<V>> values() {
    return complete(asyncMap.values());
  }

  @Override
  public Set<Map.Entry<K, Versioned<V>>> entrySet() {
    return complete(asyncMap.entrySet());
  }

  @Override
  public Versioned<V> putIfAbsent(K key, V value, Duration ttl) {
    return complete(asyncMap.putIfAbsent(key, value, ttl));
  }

  @Override
  public boolean remove(K key, V value) {
    return complete(asyncMap.remove(key, value));
  }

  @Override
  public boolean remove(K key, long version) {
    return complete(asyncMap.remove(key, version));
  }

  @Override
  public Versioned<V> replace(K key, V value) {
    return complete(asyncMap.replace(key, value));
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) {
    return complete(asyncMap.replace(key, oldValue, newValue));
  }

  @Override
  public boolean replace(K key, long oldVersion, V newValue) {
    return complete(asyncMap.replace(key, oldVersion, newValue));
  }

  @Override
  public void addListener(MapEventListener<K, V> listener, Executor executor) {
    complete(asyncMap.addListener(listener, executor));
  }

  @Override
  public void removeListener(MapEventListener<K, V> listener) {
    complete(asyncMap.removeListener(listener));
  }

  @Override
  public void addStatusChangeListener(Consumer<Status> listener) {
    asyncMap.addStatusChangeListener(listener);
  }

  @Override
  public void removeStatusChangeListener(Consumer<Status> listener) {
    asyncMap.removeStatusChangeListener(listener);
  }

  @Override
  public Collection<Consumer<Status>> statusChangeListeners() {
    return asyncMap.statusChangeListeners();
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
  public String toString() {
    return asJavaMap().toString();
  }

  @Override
  public AsyncConsistentMap<K, V> async() {
    return asyncMap;
  }

  private <T> T complete(CompletableFuture<T> future) {
    try {
      return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConsistentMapException.Interrupted();
    } catch (TimeoutException e) {
      throw new ConsistentMapException.Timeout(name());
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause());
      throw new ConsistentMapException(e.getCause());
    }
  }
}