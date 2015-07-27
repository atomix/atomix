/*
 * Copyright 2015 the original author or authors.
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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.PersistenceLevel;
import net.kuujo.copycat.ConsistencyLevel;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncMap<K, V> {

  /**
   * Checks whether the map is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the map is empty.
   */
  CompletableFuture<Boolean> isEmpty();

  /**
   * Checks whether the map is empty.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with a boolean value indicating whether the map is empty.
   */
  CompletableFuture<Boolean> isEmpty(ConsistencyLevel consistency);

  /**
   * Gets the size of the map.
   *
   * @return A completable future to be completed with the number of entries in the map.
   */
  CompletableFuture<Integer> size();

  /**
   * Gets the size of the map.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the number of entries in the map.
   */
  CompletableFuture<Integer> size(ConsistencyLevel consistency);

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> containsKey(Object key);

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> containsKey(Object key, ConsistencyLevel consistency);

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> get(Object key);

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> get(Object key, ConsistencyLevel consistency);

  /**
   * Puts a value in the map.
   *
   * @param key   The key to set.
   * @param value The value to set.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> put(K key, V value);

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param persistence The persistence in which to set the key.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> put(K key, V value, PersistenceLevel persistence);

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param ttl The time to live in milliseconds.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> put(K key, V value, long ttl);

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param ttl The time to live in milliseconds.
   * @param persistence The persistence in which to set the key.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> put(K key, V value, long ttl, PersistenceLevel persistence);

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param ttl The time to live in milliseconds.
   * @param unit The time to live unit.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> put(K key, V value, long ttl, TimeUnit unit);

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param ttl The time to live in milliseconds.
   * @param unit The time to live unit.
   * @param persistence The persistence in which to set the key.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> put(K key, V value, long ttl, TimeUnit unit, PersistenceLevel persistence);

  /**
   * Removes a value from the map.
   *
   * @param key The key to remove.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> remove(Object key);

  /**
   * Gets the value of a key or the given default value if the key does not exist.
   *
   * @param key          The key to get.
   * @param defaultValue The default value to return if the key does not exist.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> getOrDefault(Object key, V defaultValue);

  /**
   * Gets the value of a key or the given default value if the key does not exist.
   *
   * @param key          The key to get.
   * @param defaultValue The default value to return if the key does not exist.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> getOrDefault(Object key, V defaultValue, ConsistencyLevel consistency);

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> putIfAbsent(K key, V value);

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @param ttl The time to live in milliseconds.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> putIfAbsent(K key, V value, long ttl);

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @param ttl The time to live in milliseconds.
   * @param persistence The persistence in which to set the key.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> putIfAbsent(K key, V value, long ttl, PersistenceLevel persistence);

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @param ttl The time to live.
   * @param unit The time to live unit.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> putIfAbsent(K key, V value, long ttl, TimeUnit unit);

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @param ttl The time to live.
   * @param unit The time to live unit.
   * @param persistence The persistence in which to set the key.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> putIfAbsent(K key, V value, long ttl, TimeUnit unit, PersistenceLevel persistence);

  /**
   * Removes a key and value from the map.
   *
   * @param key   The key to remove.
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> remove(Object key, Object value);

  /**
   * Removes all entries from the map.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Void> clear();

}
