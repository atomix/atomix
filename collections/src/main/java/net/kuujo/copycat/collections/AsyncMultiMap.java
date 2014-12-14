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

import net.kuujo.copycat.CopycatResource;
import net.kuujo.copycat.StateMachine;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.internal.map.AsyncMultiMapState;
import net.kuujo.copycat.collections.internal.map.DefaultAsyncMultiMap;
import net.kuujo.copycat.collections.internal.map.DefaultAsyncMultiMapState;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.spi.ExecutionContext;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Asynchronous multi-map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <K> The map key type.
 * @param <V> The map entry type.
 */
public interface AsyncMultiMap<K, V> extends CopycatResource {

  /**
   * Creates a new asynchronous multimap.
   *
   * @param name The asynchronous multimap name.
   * @param <K> The multimap key type.
   * @param <V> The multimap entry type.
   * @return A new asynchronous multimap.
   */
  static <K, V> AsyncMultiMap<K, V> create(String name) {
    return create(name, Services.load("copycat.cluster", ClusterConfig.class), Services.load(String.format("copycat.multimap.%s", name), AsyncMultiMapConfig.class), ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous multimap.
   *
   * @param name The asynchronous multimap name.
   * @param cluster The cluster configuration.
   * @param <K> The multimap key type.
   * @param <V> The multimap entry type.
   * @return A new asynchronous multimap.
   */
  @SuppressWarnings("unchecked")
  static <K, V> AsyncMultiMap<K, V> create(String name, ClusterConfig cluster) {
    return create(name, cluster, Services.load(String.format("copycat.multimap.%s", name), AsyncMultiMapConfig.class), ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous multimap.
   *
   * @param name The asynchronous multimap name.
   * @param config The multimap configuration.
   * @param <K> The multimap key type.
   * @param <V> The multimap entry type.
   * @return A new asynchronous multimap.
   */
  @SuppressWarnings("unchecked")
  static <K, V> AsyncMultiMap<K, V> create(String name, AsyncMultiMapConfig config) {
    return create(name, Services.load("copycat.cluster", ClusterConfig.class), config, ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous multimap.
   *
   * @param name The asynchronous multimap name.
   * @param context The user execution context.
   * @param <K> The multimap key type.
   * @param <V> The multimap entry type.
   * @return A new asynchronous multimap.
   */
  @SuppressWarnings("unchecked")
  static <K, V> AsyncMultiMap<K, V> create(String name, ExecutionContext context) {
    return create(name, Services.load("copycat.cluster", ClusterConfig.class), Services.load(String.format("copycat.multimap.%s", name), AsyncMultiMapConfig.class), context);
  }

  /**
   * Creates a new asynchronous multimap.
   *
   * @param name The asynchronous multimap name.
   * @param cluster The cluster configuration.
   * @param config The multimap configuration.
   * @param <K> The multimap key type.
   * @param <V> The multimap entry type.
   * @return A new asynchronous multimap.
   */
  @SuppressWarnings("unchecked")
  static <K, V> AsyncMultiMap<K, V> create(String name, ClusterConfig cluster, AsyncMultiMapConfig config) {
    return create(name, cluster, config, ExecutionContext.create());
  }

  /**
   * Creates a new asynchronous multimap.
   *
   * @param name The asynchronous multimap name.
   * @param cluster The cluster configuration.
   * @param context The user execution context.
   * @param <K> The multimap key type.
   * @param <V> The multimap entry type.
   * @return A new asynchronous multimap.
   */
  @SuppressWarnings("unchecked")
  static <K, V> AsyncMultiMap<K, V> create(String name, ClusterConfig cluster, ExecutionContext context) {
    return create(name, cluster, Services.load(String.format("copycat.multimap.%s", name), AsyncMultiMapConfig.class), context);
  }

  /**
   * Creates a new asynchronous multimap.
   *
   * @param name The asynchronous multimap name.
   * @param config The multimap configuration.
   * @param context The user execution context.
   * @param <K> The multimap key type.
   * @param <V> The multimap entry type.
   * @return A new asynchronous multimap.
   */
  @SuppressWarnings("unchecked")
  static <K, V> AsyncMultiMap<K, V> create(String name, AsyncMultiMapConfig config, ExecutionContext context) {
    return create(name, Services.load("copycat.cluster", ClusterConfig.class), config, context);
  }

  /**
   * Creates a new asynchronous multimap.
   *
   * @param name The asynchronous multimap name.
   * @param cluster The cluster configuration.
   * @param config The multimap configuration.
   * @param context The user execution context.
   * @param <K> The multimap key type.
   * @param <V> The multimap entry type.
   * @return A new asynchronous multimap.
   */
  @SuppressWarnings("unchecked")
  static <K, V> AsyncMultiMap<K, V> create(String name, ClusterConfig cluster, AsyncMultiMapConfig config, ExecutionContext context) {
    return new DefaultAsyncMultiMap(StateMachine.create(name, AsyncMultiMapState.class, new DefaultAsyncMultiMapState<>(), cluster, config, context));
  }

  /**
   * Sets a key entry in the map.
   *
   * @param key The key to set.
   * @param value The entry to set
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Boolean> put(K key, V value);

  /**
   * Gets a key entry in the map.
   *
   * @param key The key to get.
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Collection<V>> get(K key);

  /**
   * Removes a key from the map.
   *
   * @param key The key to remove.
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Collection<V>> remove(K key);

  /**
   * Removes a entry from a key in the map.
   *
   * @param key The key from which to remove the entry.
   * @param value The entry to remove.
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Boolean> remove(K key, V value);

  /**
   * Checks if the map contains a key.
   *
   * @param key The key to check.
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Boolean> containsKey(K key);

  /**
   * Checks if the map contains a entry.
   *
   * @param value The entry to check.
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Boolean> containsValue(V value);

  /**
   * Checks if the map contains a key/entry pair.
   *
   * @param key The key to check.
   * @param value The entry to check.
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Boolean> containsEntry(K key, V value);

  /**
   * Gets a set of keys in the map.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Set<K>> keySet();

  /**
   * Gets a set of entries in the map.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Set<Map.Entry<K, Collection<V>>>> entrySet();

  /**
   * Gets a collection of values in the map.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Collection<V>> values();

  /**
   * Gets the current size of the map.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Integer> size();

  /**
   * Checks whether the map is empty.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Boolean> isEmpty();

  /**
   * Clears all keys from the map.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  CompletableFuture<Void> clear();

}
