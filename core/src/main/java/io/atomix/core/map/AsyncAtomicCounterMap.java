/*
 * Copyright 2015-present Open Networking Foundation
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
package io.atomix.core.map;

import io.atomix.primitive.AsyncPrimitive;
import io.atomix.primitive.DistributedPrimitive;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * An async atomic counter map dispenses monotonically increasing values associated with key.
 */
public interface AsyncAtomicCounterMap<K> extends AsyncPrimitive {

  /**
   * Increments by one the value currently associated with key, and returns the new value.
   *
   * @param key key with which the specified value is to be associated
   * @return incremented value
   */
  CompletableFuture<Long> incrementAndGet(K key);

  /**
   * Decrements by one the value currently associated with key, and returns the new value.
   *
   * @param key key with which the specified value is to be associated
   * @return updated value
   */
  CompletableFuture<Long> decrementAndGet(K key);

  /**
   * Increments by one the value currently associated with key, and returns the old value.
   *
   * @param key key with which the specified value is to be associated
   * @return previous value
   */
  CompletableFuture<Long> getAndIncrement(K key);

  /**
   * Decrements by one the value currently associated with key, and returns the old value.
   *
   * @param key key with which the specified value is to be associated
   * @return previous value
   */
  CompletableFuture<Long> getAndDecrement(K key);

  /**
   * Adds delta to the value currently associated with key, and returns the new value.
   *
   * @param key   key with which the specified value is to be associated
   * @param delta the value to add
   * @return updated value
   */
  CompletableFuture<Long> addAndGet(K key, long delta);

  /**
   * Adds delta to the value currently associated with key, and returns the old value.
   *
   * @param key   key with which the specified value is to be associated
   * @param delta the value to add
   * @return previous value
   */
  CompletableFuture<Long> getAndAdd(K key, long delta);

  /**
   * Returns the value associated with key, or zero if there is no value associated with key.
   *
   * @param key key with which the specified value is to be associated
   * @return current value
   */
  CompletableFuture<Long> get(K key);

  /**
   * Associates ewValue with key in this map, and returns the value previously
   * associated with key, or zero if there was no such value.
   *
   * @param key      key with which the specified value is to be associated
   * @param newValue the value to put
   * @return previous value or zero
   */
  CompletableFuture<Long> put(K key, long newValue);


  /**
   * If key is not already associated with a value or if key is associated with
   * zero, associate it with newValue. Returns the previous value associated with
   * key, or zero if there was no mapping for key.
   *
   * @param key      key with which the specified value is to be associated
   * @param newValue the value to put
   * @return previous value or zero
   */
  CompletableFuture<Long> putIfAbsent(K key, long newValue);

  /**
   * If (key, expectedOldValue) is currently in the map, this method replaces
   * expectedOldValue with newValue and returns true; otherwise, this method return false.
   * <p>
   * If expectedOldValue is zero, this method will succeed if (key, zero)
   * is currently in the map, or if key is not in the map at all.
   *
   * @param key              key with which the specified value is to be associated
   * @param expectedOldValue the expected value
   * @param newValue         the value to replace
   * @return true if the value was replaced, false otherwise
   */
  CompletableFuture<Boolean> replace(K key, long expectedOldValue, long newValue);

  /**
   * Removes and returns the value associated with key. If key is not
   * in the map, this method has no effect and returns zero.
   *
   * @param key key with which the specified value is to be associated
   * @return the previous value associated with the specified key or null
   */
  CompletableFuture<Long> remove(K key);

  /**
   * If (key, value) is currently in the map, this method removes it and returns
   * true; otherwise, this method returns false.
   *
   * @param key   key with which the specified value is to be associated
   * @param value the value to remove
   * @return true if the value was removed, false otherwise
   */
  CompletableFuture<Boolean> remove(K key, long value);

  /**
   * Returns the number of entries in the map.
   *
   * @return the number of entries in the map
   */
  CompletableFuture<Integer> size();

  /**
   * Returns a boolean indicating whether the map is empty.
   *
   * @return true if the map is empty, false otherwise
   */
  CompletableFuture<Boolean> isEmpty();

  /**
   * Removes all entries from the map.
   *
   * @return void
   */
  CompletableFuture<Void> clear();

  @Override
  default AtomicCounterMap<K> sync() {
    return sync(Duration.ofMillis(DistributedPrimitive.DEFAULT_OPERATION_TIMEOUT_MILLIS));
  }

  @Override
  AtomicCounterMap<K> sync(Duration operationTimeout);
}
