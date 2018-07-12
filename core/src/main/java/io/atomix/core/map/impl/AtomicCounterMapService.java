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

import io.atomix.primitive.operation.Command;
import io.atomix.primitive.operation.Query;

/**
 * Atomic counter map service.
 */
public interface AtomicCounterMapService {

  /**
   * Increments by one the value currently associated with key, and returns the new value.
   *
   * @param key key with which the specified value is to be associated
   * @return incremented value
   */
  @Command
  long incrementAndGet(String key);

  /**
   * Decrements by one the value currently associated with key, and returns the new value.
   *
   * @param key key with which the specified value is to be associated
   * @return updated value
   */
  @Command
  long decrementAndGet(String key);

  /**
   * Increments by one the value currently associated with key, and returns the old value.
   *
   * @param key key with which the specified value is to be associated
   * @return previous value
   */
  @Command
  long getAndIncrement(String key);

  /**
   * Decrements by one the value currently associated with key, and returns the old value.
   *
   * @param key key with which the specified value is to be associated
   * @return previous value
   */
  @Command
  long getAndDecrement(String key);

  /**
   * Adds delta to the value currently associated with key, and returns the new value.
   *
   * @param key   key with which the specified value is to be associated
   * @param delta the value to add
   * @return updated value
   */
  @Command
  long addAndGet(String key, long delta);

  /**
   * Adds delta to the value currently associated with key, and returns the old value.
   *
   * @param key   key with which the specified value is to be associated
   * @param delta the value to add
   * @return previous value
   */
  @Command
  long getAndAdd(String key, long delta);

  /**
   * Returns the value associated with key, or zero if there is no value associated with key.
   *
   * @param key key with which the specified value is to be associated
   * @return current value
   */
  @Query
  long get(String key);

  /**
   * Associates ewValue with key in this map, and returns the value previously
   * associated with key, or zero if there was no such value.
   *
   * @param key      key with which the specified value is to be associated
   * @param newValue the value to put
   * @return previous value or zero
   */
  @Command
  long put(String key, long newValue);

  /**
   * If key is not already associated with a value or if key is associated with
   * zero, associate it with newValue. Returns the previous value associated with
   * key, or zero if there was no mapping for key.
   *
   * @param key      key with which the specified value is to be associated
   * @param newValue the value to put
   * @return previous value or zero
   */
  @Command
  long putIfAbsent(String key, long newValue);

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
  @Command
  boolean replace(String key, long expectedOldValue, long newValue);

  /**
   * Removes and returns the value associated with key. If key is not
   * in the map, this method has no effect and returns zero.
   *
   * @param key key with which the specified value is to be associated
   * @return the previous value associated with the specified key or null
   */
  @Command
  long remove(String key);

  /**
   * If (key, value) is currently in the map, this method removes it and returns
   * true; otherwise, this method returns false.
   *
   * @param key   key with which the specified value is to be associated
   * @param value the value to remove
   * @return true if the value was removed, false otherwise
   */
  @Command("removeValue")
  boolean remove(String key, long value);

  /**
   * Returns the number of entries in the map.
   *
   * @return the number of entries in the map, including {@code 0} values
   */
  @Query
  int size();

  /**
   * If the map is empty, returns true, otherwise false.
   *
   * @return true if the map is empty.
   */
  @Query
  boolean isEmpty();

  /**
   * Clears all entries from the map.
   */
  @Command
  void clear();

}
