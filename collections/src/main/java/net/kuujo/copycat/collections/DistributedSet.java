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

import net.kuujo.copycat.PersistenceMode;
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.collections.state.SetCommands;
import net.kuujo.copycat.collections.state.SetState;
import net.kuujo.catalog.client.ConsistencyLevel;
import net.kuujo.catalog.server.StateMachine;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed set.
 *
 * @param <T> The set value type.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DistributedSet<T> extends Resource {

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return SetState.class;
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
   * Adds a value to the set with a configured persistence level.
   *
   * @param value The value to add.
   * @param persistence The persistence persistence.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, PersistenceMode persistence) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .withPersistence(persistence)
      .build());
  }

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live duration.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, Duration ttl) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .withTtl(ttl.toMillis())
      .build());
  }

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live duration.
   * @param persistence The persistence persistence.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, Duration ttl, PersistenceMode persistence) {
    return submit(SetCommands.Add.builder()
      .withValue(value.hashCode())
      .withTtl(ttl.toMillis())
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
   * Gets the set count.
   *
   * @return A completable future to be completed with the set count.
   */
  public CompletableFuture<Integer> size() {
    return submit(SetCommands.Size.builder().build());
  }

  /**
   * Gets the set count.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the set count.
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
