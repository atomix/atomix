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
import net.kuujo.copycat.spi.Protocol;

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
    return create(name, Services.load("cluster"), Services.load("protocol"));
  }

  /**
   * Creates a new asynchronous multimap.
   *
   * @param name The asynchronous multimap name.
   * @param config The cluster configuration.
   * @param protocol The cluster protocol.
   * @param <K> The multimap key type.
   * @param <V> The multimap entry type.
   * @return A new asynchronous multimap.
   */
  @SuppressWarnings("unchecked")
  static <K, V> AsyncMultiMap<K, V> create(String name, ClusterConfig config, Protocol protocol) {
    return new DefaultAsyncMultiMap(StateMachine.create(name, AsyncMultiMapState.class, new DefaultAsyncMultiMapState<>(), config, protocol));
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
