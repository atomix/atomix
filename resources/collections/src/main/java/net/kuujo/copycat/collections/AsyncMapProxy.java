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
import java.util.function.Function;

/**
 * Asynchronous map proxy.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AsyncMapProxy<K, V> {

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
  CompletableFuture<Boolean> containsKey(Object key);

  /**
   * Checks whether the map contains a value.
   *
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> containsValue(Object value);

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> get(Object key);

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> put(K key, V value);

  /**
   * Removes a value from the map.
   *
   * @param key The key to remove.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> remove(Object key);

  /**
   * Puts a map of values in the map.
   *
   * @param m The put to put.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Void> putAll(Map<? extends K, ? extends V> m);

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
  CompletableFuture<V> getOrDefault(Object key, V defaultValue);

  /**
   * Replaces values in the map.
   *
   * @param function The serializable function with which to replace values.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Void> replaceAll(BiFunction<? super K, ? super V, ? extends V> function);

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key The key to set.
   * @param value The value to set if the given key does not exist.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> putIfAbsent(K key, V value);

  /**
   * Removes a key and value from the map.
   *
   * @param key The key to remove.
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> remove(Object key, Object value);

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
  CompletableFuture<V> replace(K key, V value);

  /**
   * Computes the value of a key if the key is absent from the map.
   *
   * @param key The key to compute.
   * @param mappingFunction The serializable function with which to compute the value.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction);

  /**
   * Computes the value of a key if the key is present in the map.
   *
   * @param key The key to compute.
   * @param remappingFunction The serializable function with which to compute the value.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

  /**
   * Computes the value of a key in the map.
   *
   * @param key The key to compute.
   * @param remappingFunction The The serializable function with which to compute the value.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction);

  /**
   * Merges values in the map.
   *
   * @param key The key to merge.
   * @param value The value to merge.
   * @param remappingFunction The function with which to merge the value.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction);

}
