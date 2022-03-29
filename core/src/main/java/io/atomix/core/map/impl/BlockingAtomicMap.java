// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.core.map.impl;

import java.time.Duration;
import java.util.ConcurrentModificationException;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import com.google.common.base.Throwables;
import io.atomix.core.collection.DistributedCollection;
import io.atomix.core.collection.impl.BlockingDistributedCollection;
import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.core.map.AtomicMap;
import io.atomix.core.map.AtomicMapEventListener;
import io.atomix.core.set.DistributedSet;
import io.atomix.core.set.impl.BlockingDistributedSet;
import io.atomix.primitive.PrimitiveException;
import io.atomix.primitive.PrimitiveState;
import io.atomix.primitive.Synchronous;
import io.atomix.utils.concurrent.Retries;
import io.atomix.utils.time.Versioned;

/**
 * Default implementation of {@code ConsistentMap}.
 *
 * @param <K> type of key.
 * @param <V> type of value.
 */
public class BlockingAtomicMap<K, V> extends Synchronous<AsyncAtomicMap<K, V>> implements AtomicMap<K, V> {

  private static final int MAX_DELAY_BETWEEN_RETRY_MILLS = 50;
  private final AsyncAtomicMap<K, V> asyncMap;
  private final long operationTimeoutMillis;

  public BlockingAtomicMap(AsyncAtomicMap<K, V> asyncMap, long operationTimeoutMillis) {
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
  public Map<K, Versioned<V>> getAllPresent(Iterable<K> keys) {
    return complete(asyncMap.getAllPresent(keys));
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
        PrimitiveException.ConcurrentModification.class,
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
  public DistributedSet<K> keySet() {
    return new BlockingDistributedSet<K>(asyncMap.keySet(), operationTimeoutMillis);
  }

  @Override
  public DistributedCollection<Versioned<V>> values() {
    return new BlockingDistributedCollection<>(asyncMap.values(), operationTimeoutMillis);
  }

  @Override
  public DistributedSet<Map.Entry<K, Versioned<V>>> entrySet() {
    return new BlockingDistributedSet<>(asyncMap.entrySet(), operationTimeoutMillis);
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
  public long lock(K key) {
    return complete(asyncMap.lock(key));
  }

  @Override
  public OptionalLong tryLock(K key) {
    return complete(asyncMap.tryLock(key));
  }

  @Override
  public OptionalLong tryLock(K key, Duration timeout) {
    return complete(asyncMap.tryLock(key, timeout));
  }

  @Override
  public boolean isLocked(K key) {
    return complete(asyncMap.isLocked(key));
  }

  @Override
  public boolean isLocked(K key, long version) {
    return complete(asyncMap.isLocked(key, version));
  }

  @Override
  public void unlock(K key) {
    complete(asyncMap.unlock(key));
  }

  @Override
  public void addListener(AtomicMapEventListener<K, V> listener, Executor executor) {
    complete(asyncMap.addListener(listener, executor));
  }

  @Override
  public void removeListener(AtomicMapEventListener<K, V> listener) {
    complete(asyncMap.removeListener(listener));
  }

  @Override
  public void addStateChangeListener(Consumer<PrimitiveState> listener) {
    asyncMap.addStateChangeListener(listener);
  }

  @Override
  public void removeStateChangeListener(Consumer<PrimitiveState> listener) {
    asyncMap.removeStateChangeListener(listener);
  }

  @Override
  public AsyncAtomicMap<K, V> async() {
    return asyncMap;
  }

  protected <T> T complete(CompletableFuture<T> future) {
    try {
      return future.get(operationTimeoutMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new PrimitiveException.Interrupted();
    } catch (TimeoutException e) {
      throw new PrimitiveException.Timeout();
    } catch (ExecutionException e) {
      Throwable cause = Throwables.getRootCause(e);
      if (cause instanceof PrimitiveException) {
        throw (PrimitiveException) cause;
      } else if (cause instanceof ConcurrentModificationException) {
        throw (ConcurrentModificationException) cause;
      } else {
        throw new PrimitiveException(cause);
      }
    }
  }
}
