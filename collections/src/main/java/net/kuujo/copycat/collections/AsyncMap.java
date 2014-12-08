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

import net.kuujo.copycat.Coordinator;
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.collections.internal.DefaultAsyncMap;
import net.kuujo.copycat.internal.DefaultCoordinator;
import net.kuujo.copycat.internal.util.Services;
import net.kuujo.copycat.log.InMemoryLog;
import net.kuujo.copycat.spi.ExecutionContext;
import net.kuujo.copycat.spi.Protocol;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Asynchronous map.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 *
 * @param <K> The map key type.
 * @param <V> The map value type.
 */
public interface AsyncMap<K, V> extends Resource {

  /**
   * Creates a new asynchronous map.
   *
   * @param name The asynchronous map name.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map.
   */
  static <K, V> AsyncMap<K, V> create(String name) {
    return create(name, Services.load("cluster"), Services.load("protocol"));
  }

  /**
   * Creates a new asynchronous map.
   *
   * @param name The asynchronous map name.
   * @param config The cluster configuration.
   * @param protocol The cluster protocol.
   * @param <K> The map key type.
   * @param <V> The map value type.
   * @return A new asynchronous map.
   */
  @SuppressWarnings("unchecked")
  static <K, V> AsyncMap<K, V> create(String name, ClusterConfig config, Protocol protocol) {
    ExecutionContext executor = ExecutionContext.create();
    Coordinator coordinator = new DefaultCoordinator(config, protocol, new InMemoryLog(), executor);
    try {
      return coordinator.<AsyncMap<K, V>>createResource(name, resource -> new InMemoryLog(), (resource, coord, cluster, context) -> {
        return (AsyncMap<K, V>) new DefaultAsyncMap<>(resource, coord, cluster, context).withShutdownTask(coordinator::close);
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Sets a key value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> put(K key, V value);

  /**
   * Gets a key value from the map.
   *
   * @param key The key to get.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> get(K key);

  /**
   * Removes a key from the map.
   *
   * @param key The key to remove.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<V> remove(K key);

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Boolean> containsKey(K key);

  /**
   * Gets a set of keys in the map.
   *
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Set<K>> keySet();

  /**
   * Gets a set of entries in the map.
   *
   * @return A completable future to be completed with the entry set once complete.
   */
  CompletableFuture<Set<Map.Entry<K, V>>> entrySet();

  /**
   * Gets a collection of values in the map.
   *
   * @return A completable future to be completed with the result once complete.
   */
  CompletableFuture<Collection<V>> values();

  /**
   * Gets the current size of the map.
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
   * Clears all keys from the map.
   *
   * @return A completable future to be completed once the map has been cleared.
   */
  CompletableFuture<Void> clear();

}
