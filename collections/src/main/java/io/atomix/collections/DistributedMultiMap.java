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

import io.atomix.DistributedResource;
import io.atomix.catalyst.util.Assert;
import io.atomix.collections.state.MultiMapCommands;
import io.atomix.collections.state.MultiMapState;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.server.StateMachine;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed multi-map.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class DistributedMultiMap<K, V> extends DistributedResource<DistributedMultiMap<K, V>> {
  private Command.ConsistencyLevel commandConsistency = Command.ConsistencyLevel.LINEARIZABLE;
  private Query.ConsistencyLevel queryConsistency = Query.ConsistencyLevel.LINEARIZABLE;

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return MultiMapState.class;
  }

  /**
   * Sets the default write consistency level.
   *
   * @param consistency The default write consistency level.
   * @throws java.lang.NullPointerException If the consistency level is {@code null}
   */
  public void setDefaultCommandConsistency(Command.ConsistencyLevel consistency) {
    this.commandConsistency = Assert.notNull(consistency, "consistency");
  }

  /**
   * Sets the default write consistency level, returning the resource for method chaining.
   *
   * @param consistency The default write consistency level.
   * @return The reference.
   * @throws java.lang.NullPointerException If the consistency level is {@code null}
   */
  public DistributedMultiMap<K, V> withDefaultCommandConsistency(Command.ConsistencyLevel consistency) {
    setDefaultCommandConsistency(consistency);
    return this;
  }

  /**
   * Returns the default write consistency level.
   *
   * @return The default write consistency level.
   */
  public Command.ConsistencyLevel getDefaultCommandConsistency() {
    return commandConsistency;
  }

  /**
   * Sets the default read consistency level.
   *
   * @param consistency The default read consistency level.
   * @throws java.lang.NullPointerException If the consistency level is {@code null}
   */
  public void setDefaultQueryConsistency(Query.ConsistencyLevel consistency) {
    this.queryConsistency = Assert.notNull(consistency, "consistency");
  }

  /**
   * Sets the default read consistency level, returning the resource for method chaining.
   *
   * @param consistency The default read consistency level.
   * @return The reference.
   * @throws java.lang.NullPointerException If the consistency level is {@code null}
   */
  public DistributedMultiMap<K, V> withDefaultQueryConsistency(Query.ConsistencyLevel consistency) {
    setDefaultQueryConsistency(consistency);
    return this;
  }

  /**
   * Returns the default read consistency level.
   *
   * @return The default read consistency level.
   */
  public Query.ConsistencyLevel getDefaultQueryConsistency() {
    return queryConsistency;
  }

  /**
   * Checks whether the map is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the map is empty.
   */
  public CompletableFuture<Boolean> isEmpty() {
    return submit(MultiMapCommands.IsEmpty.builder().build());
  }

  /**
   * Gets the number of key-value pairs in the map.
   *
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size() {
    return submit(MultiMapCommands.Size.builder()
      .build());
  }

  /**
   * Gets the number of values for the given key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size(K key) {
    return submit(MultiMapCommands.Size.builder()
      .withKey(key)
      .build());
  }

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsKey(K key) {
    return submit(MultiMapCommands.ContainsKey.builder()
      .withKey(key)
      .build());
  }

  /**
   * Checks whether the map contains an entry.
   *
   * @param key The key to check.
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsEntry(K key, V value) {
    return submit(MultiMapCommands.ContainsEntry.builder()
      .withKey(key)
      .withValue(value)
      .build());
  }

  /**
   * Checks whether the map contains a value.
   *
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsValue(V value) {
    return submit(MultiMapCommands.ContainsValue.builder()
      .withValue(value)
      .build());
  }

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Collection<V>> get(K key) {
    return submit(MultiMapCommands.Get.builder()
      .withKey(key)
      .build())
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
    return submit(MultiMapCommands.Put.builder()
      .withKey(key)
      .withValue(value)
      .build())
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
    return submit(MultiMapCommands.Put.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl.toMillis())
      .build());
  }

  /**
   * Removes a value from the map.
   *
   * @param key The key to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Collection<V>> remove(Object key) {
    return submit(MultiMapCommands.Remove.builder()
      .withKey(key)
      .build())
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
    return submit(MultiMapCommands.Remove.builder()
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
    return submit(MultiMapCommands.Clear.builder().build());
  }

}
