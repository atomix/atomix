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
package io.atomix.collections;

import io.atomix.Resource;
import io.atomix.collections.state.MapCommands;
import io.atomix.collections.state.MapState;
import io.atomix.copycat.server.StateMachine;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed map.
 *
 * @param <K> The map key type.
 * @param <V> The map entry type.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DistributedMap<K, V> extends Resource<DistributedMap<K, V>> {

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return MapState.class;
  }

  /**
   * Checks whether the map is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the map is empty.
   */
  public CompletableFuture<Boolean> isEmpty() {
    return submit(MapCommands.IsEmpty.builder().build());
  }

  /**
   * Gets the count of the map.
   *
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size() {
    return submit(MapCommands.Size.builder().build());
  }

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsKey(Object key) {
    return submit(MapCommands.ContainsKey.builder()
      .withKey(key)
      .build());
  }

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> get(Object key) {
    return submit(MapCommands.Get.builder()
      .withKey(key)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map.
   *
   * @param key   The key to set.
   * @param value The value to set.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value) {
    return submit(MapCommands.Put.builder()
      .withKey(key)
      .withValue(value)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param ttl The duration after which to expire the key.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> put(K key, V value, Duration ttl) {
    return submit(MapCommands.Put.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl.toMillis())
      .build())
      .thenApply(result -> result);
  }

  /**
   * Removes a value from the map.
   *
   * @param key The key to remove.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> remove(Object key) {
    return submit(MapCommands.Remove.builder()
      .withKey(key)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Gets the value of a key or the given default value if the key does not exist.
   *
   * @param key          The key to get.
   * @param defaultValue The default value to return if the key does not exist.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> getOrDefault(Object key, V defaultValue) {
    return submit(MapCommands.GetOrDefault.builder()
      .withKey(key)
      .withDefaultValue(defaultValue)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value) {
    return submit(MapCommands.PutIfAbsent.builder()
      .withKey(key)
      .withValue(value)
      .build())
      .thenApply(result -> (V) result);
  }

  /**
   * Puts a value in the map if the given key does not exist.
   *
   * @param key   The key to set.
   * @param value The value to set if the given key does not exist.
   * @param ttl The time to live duration.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<V> putIfAbsent(K key, V value, Duration ttl) {
    return submit(MapCommands.PutIfAbsent.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl.toMillis())
      .build())
      .thenApply(result -> result);
  }

  /**
   * Removes a key and value from the map.
   *
   * @param key   The key to remove.
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(Object key, Object value) {
    return submit(MapCommands.Remove.builder()
      .withKey(key)
      .withValue(value)
      .build())
      .thenApply(result -> (boolean) result);
  }

  /**
   * Removes all entries from the map.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear() {
    return submit(MapCommands.Clear.builder().build());
  }

}
