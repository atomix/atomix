// SPDX-FileCopyrightText: 2016-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0


package io.atomix.core.map.impl;

import io.atomix.core.map.AsyncAtomicMap;
import io.atomix.utils.concurrent.Futures;
import io.atomix.utils.time.Versioned;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * An unmodifiable {@link AsyncAtomicMap}.
 * <p>
 * Any attempt to update the map through this instance will cause the
 * operation to be completed with an {@link UnsupportedOperationException}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class UnmodifiableAsyncAtomicMap<K, V> extends DelegatingAsyncAtomicMap<K, V> {

  private static final String ERROR_MSG = "map updates are not allowed";

  public UnmodifiableAsyncAtomicMap(AsyncAtomicMap<K, V> backingMap) {
    super(backingMap);
  }

  @Override
  public CompletableFuture<Versioned<V>> computeIf(K key,
                                                   Predicate<? super V> condition,
                                                   BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(""));
  }

  @Override
  public CompletableFuture<Versioned<V>> put(K key, V value) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Versioned<V>> putAndGet(K key, V value) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Versioned<V>> remove(K key) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Void> clear() {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Versioned<V>> putIfAbsent(K key, V value) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, V value) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Boolean> remove(K key, long version) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Versioned<V>> replace(K key, V value) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, V oldValue, V newValue) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }

  @Override
  public CompletableFuture<Boolean> replace(K key, long oldVersion, V newValue) {
    return Futures.exceptionalFuture(new UnsupportedOperationException(ERROR_MSG));
  }
}
