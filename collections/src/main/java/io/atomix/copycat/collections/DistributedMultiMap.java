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
package io.atomix.copycat.collections;

import io.atomix.catalogue.client.Command;
import io.atomix.catalogue.client.Query;
import io.atomix.catalogue.server.StateMachine;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Resource;
import io.atomix.copycat.collections.state.MultiMapCommands;
import io.atomix.copycat.collections.state.MultiMapState;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed multi-map.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class DistributedMultiMap<K, V> extends Resource {
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
    return isEmpty(queryConsistency);
  }

  /**
   * Checks whether the map is empty.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with a boolean value indicating whether the map is empty.
   */
  public CompletableFuture<Boolean> isEmpty(Query.ConsistencyLevel consistency) {
    return submit(MultiMapCommands.IsEmpty.builder().withConsistency(consistency).build());
  }

  /**
   * Gets the number of key-value pairs in the map.
   *
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size() {
    return size(queryConsistency);
  }

  /**
   * Gets the number of key-value pairs in the map.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size(Query.ConsistencyLevel consistency) {
    return submit(MultiMapCommands.Size.builder()
      .withConsistency(consistency)
      .build());
  }

  /**
   * Gets the number of values for the given key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size(K key) {
    return size(key, queryConsistency);
  }

  /**
   * Gets the number of values for the given key.
   *
   * @param key The key to check.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the number of entries in the map.
   */
  public CompletableFuture<Integer> size(K key, Query.ConsistencyLevel consistency) {
    return submit(MultiMapCommands.Size.builder()
      .withKey(key)
      .withConsistency(consistency)
      .build());
  }

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsKey(K key) {
    return containsKey(key, queryConsistency);
  }

  /**
   * Checks whether the map contains a key.
   *
   * @param key The key to check.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsKey(K key, Query.ConsistencyLevel consistency) {
    return submit(MultiMapCommands.ContainsKey.builder()
      .withKey(key)
      .withConsistency(consistency)
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
    return containsEntry(key, value, queryConsistency);
  }

  /**
   * Checks whether the map contains an entry.
   *
   * @param key The key to check.
   * @param value The value to check.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsEntry(K key, V value, Query.ConsistencyLevel consistency) {
    return submit(MultiMapCommands.ContainsEntry.builder()
      .withKey(key)
      .withValue(value)
      .withConsistency(consistency)
      .build());
  }

  /**
   * Checks whether the map contains a value.
   *
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsValue(V value) {
    return containsValue(value, queryConsistency);
  }

  /**
   * Checks whether the map contains a value.
   *
   * @param value The value to check.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> containsValue(V value, Query.ConsistencyLevel consistency) {
    return submit(MultiMapCommands.ContainsValue.builder()
      .withValue(value)
      .withConsistency(consistency)
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
    return get(key, queryConsistency);
  }

  /**
   * Gets a value from the map.
   *
   * @param key The key to get.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Collection<V>> get(K key, Query.ConsistencyLevel consistency) {
    return submit(MultiMapCommands.Get.builder()
      .withKey(key)
      .withConsistency(consistency)
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
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> put(K key, V value) {
    return put(key, value, commandConsistency);
  }

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param consistency The command consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> put(K key, V value, Command.ConsistencyLevel consistency) {
    return submit(MultiMapCommands.Put.builder()
      .withKey(key)
      .withValue(value)
      .withConsistency(consistency)
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
    return put(key, value, ttl, commandConsistency);
  }

  /**
   * Puts a value in the map.
   *
   * @param key The key to set.
   * @param value The value to set.
   * @param ttl The time to live duration.
   * @param consistency The command consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> put(K key, V value, Duration ttl, Command.ConsistencyLevel consistency) {
    return submit(MultiMapCommands.Put.builder()
      .withKey(key)
      .withValue(value)
      .withTtl(ttl.toMillis())
      .withConsistency(consistency)
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
  public CompletableFuture<Collection<V>> remove(Object key) {
    return remove(key, commandConsistency);
  }

  /**
   * Removes a value from the map.
   *
   * @param key The key to remove.
   * @param consistency The consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Collection<V>> remove(Object key, Command.ConsistencyLevel consistency) {
    return submit(MultiMapCommands.Remove.builder()
      .withKey(key)
      .withConsistency(consistency)
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
    return remove(key, value, commandConsistency);
  }

  /**
   * Removes a key and value from the map.
   *
   * @param key   The key to remove.
   * @param value The value to remove.
   * @param consistency The consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> remove(Object key, Object value, Command.ConsistencyLevel consistency) {
    return submit(MultiMapCommands.Remove.builder()
      .withKey(key)
      .withValue(value)
      .withConsistency(consistency)
      .build())
      .thenApply(result -> result);
  }

  /**
   * Removes all entries from the map.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear() {
    return clear(commandConsistency);
  }

  /**
   * Removes all entries from the map.
   *
   * @param consistency The consistency level.
   * @return A completable future to be completed once the operation is complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> clear(Command.ConsistencyLevel consistency) {
    return submit(MultiMapCommands.Clear.builder()
      .withConsistency(consistency)
      .build());
  }

}
