/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.core.transaction;

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitive;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous transactional map.
 */
public interface AsyncTransactionalMap<K, V> extends AsyncPrimitive {

  /**
   * Returns the value to which the specified key is mapped, or null if this
   * map contains no mapping for the key.
   *
   * @param key the key whose associated value is to be returned
   * @return the value to which the specified key is mapped, or null if
   * this map contains no mapping for the key
   */
  CompletableFuture<V> get(K key);

  /**
   * Returns true if this map contains a mapping for the specified key.
   *
   * @param key key whose presence in this map to be tested
   * @return true if this map contains a mapping for the specified key
   */
  CompletableFuture<Boolean> containsKey(K key);

  /**
   * Associates the specified value with the specified key in this map (optional operation).
   * If the map previously contained a mapping for the key, the old value is replaced by the
   * specified value.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with key, or null if there was
   * no mapping for key.
   */
  CompletableFuture<V> put(K key, V value);

  /**
   * Removes the mapping for a key from this map if it is present (optional operation).
   *
   * @param key key whose value is to be removed from the map
   * @return the value to which this map previously associated the key,
   * or null if the map contained no mapping for the key.
   */
  CompletableFuture<V> remove(K key);

  /**
   * If the specified key is not already associated with a value
   * associates it with the given value and returns null, else returns the current value.
   *
   * @param key   key with which the specified value is to be associated
   * @param value value to be associated with the specified key
   * @return the previous value associated with the specified key or null
   * if key does not already mapped to a value.
   */
  CompletableFuture<V> putIfAbsent(K key, V value);

  /**
   * Removes the entry for the specified key only if it is currently
   * mapped to the specified value.
   *
   * @param key   key with which the specified value is associated
   * @param value value expected to be associated with the specified key
   * @return true if the value was removed
   */
  CompletableFuture<Boolean> remove(K key, V value);

  /**
   * Replaces the entry for the specified key only if currently mapped
   * to the specified value.
   *
   * @param key      key with which the specified value is associated
   * @param oldValue value expected to be associated with the specified key
   * @param newValue value to be associated with the specified key
   * @return true if the value was replaced
   */
  CompletableFuture<Boolean> replace(K key, V oldValue, V newValue);

  @Override
  default TransactionalMap<K, V> sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  TransactionalMap<K, V> sync(Duration operationTimeout);
}
