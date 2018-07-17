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
