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
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.Stateful;
import net.kuujo.copycat.collections.state.SetCommands;
import net.kuujo.copycat.collections.state.SetState;
import net.kuujo.copycat.raft.ConsistencyLevel;
import net.kuujo.copycat.resource.ResourceContext;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Distributed set.
 *
 * @param <T> The set value type.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Stateful(SetState.class)
public class DistributedSet<T> extends Resource {

  public DistributedSet(ResourceContext context) {
    super(context);
  }

  /**
   * Adds a value to the set.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> add(T value) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .build());
  }

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param persistence The persistence persistence.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, PersistenceLevel persistence) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .withPersistence(persistence)
      .build());
  }

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live in milliseconds.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, long ttl) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .withTtl(ttl)
      .build());
  }

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live.
   * @param unit The time to live unit.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, long ttl, TimeUnit unit) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .withTtl(ttl, unit)
      .build());
  }

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live in milliseconds.
   * @param persistence The persistence persistence.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, long ttl, PersistenceLevel persistence) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .withTtl(ttl)
      .withPersistence(persistence)
      .build());
  }

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live.
   * @param unit The time to live unit.
   * @param persistence The persistence persistence.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, long ttl, TimeUnit unit, PersistenceLevel persistence) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .withTtl(ttl, unit)
      .withPersistence(persistence)
      .build());
  }

  /**
   * Removes a value from the set.
   *
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(T value) {
    return submit(SetCommands.Remove.builder()
      .withValue(value.hashCode())
      .build());
  }

  /**
   * Checks whether the set contains a value.
   *
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> contains(Object value) {
    return submit(SetCommands.Contains.builder()
      .withValue(value.hashCode())
      .build());
  }

  /**
   * Checks whether the set contains a value.
   *
   * @param value The value to check.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> contains(Object value, ConsistencyLevel consistency) {
    return submit(SetCommands.Contains.builder()
      .withValue(value.hashCode())
      .withConsistency(consistency)
      .build());
  }

  /**
   * Gets the set size.
   *
   * @return A completable future to be completed with the set size.
   */
  public CompletableFuture<Integer> size() {
    return submit(SetCommands.Size.builder().build());
  }

  /**
   * Gets the set size.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the set size.
   */
  public CompletableFuture<Integer> size(ConsistencyLevel consistency) {
    return submit(SetCommands.Size.builder().withConsistency(consistency).build());
  }

  /**
   * Checks whether the set is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the set is empty.
   */
  public CompletableFuture<Boolean> isEmpty() {
    return submit(SetCommands.IsEmpty.builder().build());
  }

  /**
   * Checks whether the set is empty.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with a boolean value indicating whether the set is empty.
   */
  public CompletableFuture<Boolean> isEmpty(ConsistencyLevel consistency) {
    return submit(SetCommands.IsEmpty.builder().withConsistency(consistency).build());
  }

  /**
   * Removes all values from the set.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear() {
    return submit(SetCommands.Clear.builder().build());
  }

}
