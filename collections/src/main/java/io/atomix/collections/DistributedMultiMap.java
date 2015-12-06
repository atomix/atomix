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

import io.atomix.collections.state.MultiMapCommands;
import io.atomix.collections.state.MultiMapState;
import io.atomix.copycat.client.RaftClient;
import io.atomix.resource.Consistency;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceType;
import io.atomix.resource.ResourceTypeInfo;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed multi-map.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-12, stateMachine=MultiMapState.class)
public class DistributedMultiMap<K, V> extends Resource {
  public static final ResourceType<DistributedMultiMap> TYPE = new ResourceType<>(DistributedMultiMap.class);

  public DistributedMultiMap(RaftClient client) {
    super(client);
  }

  @Override
  public ResourceType type() {
    return TYPE;
  }

  @Override
  public DistributedMultiMap<K, V> with(Consistency consistency) {
    super.with(consistency);
    return this;
  }

  /**
   * Checks whether the map is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the map is empty.
   */
  public CompletableFuture<Boolean> isEmpty() {
    return submit(new MultiMapCommands.IsEmpty());
  }

  /**
   * Gets the number of key-value pairs in the map.
   *
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size() {
    return submit(new MultiMapCommands.Size());
  }

  /**
   * Gets the number of values for the given key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size(K key) {
    return submit(new MultiMapCommands.Size(key));
  }

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsKey(K key) {
    return submit(new MultiMapCommands.ContainsKey(key));
  }

  /**
   * Checks whether the map contains an entry.
   *
   * @param key The key to check.
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsEntry(K key, V value) {
    return submit(new MultiMapCommands.ContainsEntry(key, value));
  }

  /**
   * Checks whether the map contains a value.
   *
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsValue(V value) {
    return submit(new MultiMapCommands.ContainsValue(value));
  }

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Collection<V>> get(K key) {
    return submit(new MultiMapCommands.Get(key))
      .thenApply(result -> result);
  }

  /**
   * Puts a value in the map.
   *
   * @param key   The key to set.
   * @param value The value to set.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> put(K key, V value) {
    return submit(new MultiMapCommands.Put(key, value))
      .thenApply(result -> result);
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
  public CompletableFuture<Boolean> put(K key, V value, Duration ttl) {
    return submit(new MultiMapCommands.Put(key, value, ttl.toMillis()));
  }

  /**
   * Removes a value from the map.
   *
   * @param key The key to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Collection<V>> remove(Object key) {
    return submit(new MultiMapCommands.Remove(key))
      .thenApply(result -> (Collection) result);
  }

  /**
   * Removes a key and value from the map.
   *
   * @param key   The key to remove.
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(Object key, Object value) {
    return submit(new MultiMapCommands.Remove(key, value))
      .thenApply(result -> (boolean) result);
  }

  /**
   * Removes all instances of a value from the map.
   *
   * @param value The value to remove.
   * @return A completable future to be completed once the value has been removed.
   */
  public CompletableFuture<Void> removeValue(Object value) {
    return submit(new MultiMapCommands.RemoveValue(value));
  }

  /**
   * Removes all entries from the map.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear() {
    return submit(new MultiMapCommands.Clear());
  }

}
