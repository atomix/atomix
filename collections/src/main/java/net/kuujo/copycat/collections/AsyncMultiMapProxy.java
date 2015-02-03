/*
 * Copyright 2014 the original author or authors.
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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

/**
 * Asynchronous multimap proxy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncMultiMapProxy<K, V> {

  /**
   * Gets the map size.
   *
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Integer> size();

  /**
   * Checks whether the map is empty.
   *
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> isEmpty();

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> containsKey(K key);

  /**
   * Checks whether the map contains a value.
   *
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> containsValue(V value);

  /**
   * Checks whether the map contains an entry.
   *
   * @param key The key to check.
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> containsEntry(K key, V value);

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Collection<V>> get(K key);

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Collection<V>> put(K key, V value);

  /**
   * Removes a value from the map.
   *
   * @param key The key to remove.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Collection<V>> remove(K key);

  /**
   * Removes a key and value from the map.
   *
   * @param key The key to remove.
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> remove(K key, V value);

  /**
   * Puts a map of values in the map.
   *
   * @param m The put to put.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Void> putAll(Map<? extends K, ? extends Collection<V>> m);

  /**
   * Clears the map.
   *
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Void> clear();

  /**
   * Gets a set of keys in the map.
   *
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Set<K>> keySet();

  /**
   * Gets a collection of values in the map.
   *
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Collection<V>> values();

  /**
   * Gets a set of entries in the map.
   *
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Set<Map.Entry<K, V>>> entrySet();

  /**
   * Gets the value of a key or the given default value if the key does not exist.
   *
   * @param key The key to get.
   * @param defaultValue The default value to return if the key does not exist.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Collection<V>> getOrDefault(K key, Collection<V> defaultValue);

  /**
   * Replaces values in the map.
   *
   * @param function The serializable function with which to replace values.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Void> replaceAll(BiFunction<? super K, ? super Collection<V>, ? extends Collection<V>> function);

  /**
   * Replaces a key and value in the map.
   *
   * @param key The key to replace.
   * @param oldValue The value to replace.
   * @param newValue The value with which to replace the given key and value.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> replace(K key, V oldValue, V newValue);

  /**
   * Replaces a key with the given value.
   *
   * @param key The key to replace.
   * @param value The value with which to replace the given key.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Collection<V>> replace(K key, Collection<V> value);

}
