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
import io.atomix.copycat.collections.state.QueueCommands;
import io.atomix.copycat.collections.state.QueueState;

import java.util.concurrent.CompletableFuture;

/**
 * Distributed queue.
 *
 * @param <T> The queue value type.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DistributedQueue<T> extends Resource {
  private Command.ConsistencyLevel commandConsistency = Command.ConsistencyLevel.LINEARIZABLE;
  private Query.ConsistencyLevel queryConsistency = Query.ConsistencyLevel.LINEARIZABLE;

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return QueueState.class;
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
   * @return The queue.
   * @throws java.lang.NullPointerException If the consistency level is {@code null}
   */
  public DistributedQueue<T> withDefaultCommandConsistency(Command.ConsistencyLevel consistency) {
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
   * @return The queue.
   * @throws java.lang.NullPointerException If the consistency level is {@code null}
   */
  public DistributedQueue<T> withDefaultQueryConsistency(Query.ConsistencyLevel consistency) {
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
   * Adds a value to the queue.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> add(T value) {
    return add(value, commandConsistency);
  }

  /**
   * Adds a value to the queue.
   *
   * @param value The value to add.
   * @param consistency The command consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> add(T value, Command.ConsistencyLevel consistency) {
    return submit(QueueCommands.Add.builder()
      .withValue(value)
      .withConsistency(consistency)
      .build());
  }

  /**
   * Adds a value to the queue.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> offer(T value) {
    return offer(value, commandConsistency);
  }

  /**
   * Adds a value to the queue.
   *
   * @param value The value to add.
   * @param consistency The command consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> offer(T value, Command.ConsistencyLevel consistency) {
    return submit(QueueCommands.Offer.builder()
      .withValue(value)
      .withConsistency(consistency)
      .build());
  }

  /**
   * Removes a value from the queue.
   *
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<T> peek(T value) {
    return peek(value, commandConsistency);
  }

  /**
   * Removes a value from the queue.
   *
   * @param value The value to remove.
   * @param consistency The command consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> peek(T value, Command.ConsistencyLevel consistency) {
    return submit(QueueCommands.Peek.builder()
      .withValue(value)
      .withConsistency(consistency)
      .build()).thenApply(v -> (T) v);
  }

  /**
   * Removes a value from the queue.
   *
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<T> poll(T value) {
    return poll(value, commandConsistency);
  }

  /**
   * Removes a value from the queue.
   *
   * @param value The value to remove.
   * @param consistency The command consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> poll(T value, Command.ConsistencyLevel consistency) {
    return submit(QueueCommands.Poll.builder()
      .withValue(value)
      .withConsistency(consistency)
      .build()).thenApply(v -> (T) v);
  }

  /**
   * Removes a value from the queue.
   *
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<T> element(T value) {
    return element(value, commandConsistency);
  }

  /**
   * Removes a value from the queue.
   *
   * @param value The value to remove.
   * @param consistency The command consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> element(T value, Command.ConsistencyLevel consistency) {
    return submit(QueueCommands.Element.builder()
      .withValue(value)
      .withConsistency(consistency)
      .build()).thenApply(v -> (T) v);
  }

  /**
   * Removes a value from the queue.
   *
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Object> remove() {
    return remove(commandConsistency);
  }

  /**
   * Removes a value from the queue.
   *
   * @param consistency The command consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Object> remove(Command.ConsistencyLevel consistency) {
    return submit(QueueCommands.Remove.builder()
      .withConsistency(consistency)
      .build()).thenApply(v -> (T) v);
  }

  /**
   * Removes a value from the queue.
   *
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(T value) {
    return remove(value, commandConsistency);
  }

  /**
   * Removes a value from the queue.
   *
   * @param value The value to remove.
   * @param consistency The command consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(T value, Command.ConsistencyLevel consistency) {
    return submit(QueueCommands.Remove.builder()
      .withValue(value)
      .withConsistency(consistency)
      .build()).thenApply(v -> (boolean) v);
  }

  /**
   * Checks whether the queue contains a value.
   *
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> contains(Object value) {
    return submit(QueueCommands.Contains.builder()
      .withValue(value)
      .build());
  }

  /**
   * Checks whether the queue contains a value.
   *
   * @param value The value to check.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> contains(Object value, Query.ConsistencyLevel consistency) {
    return submit(QueueCommands.Contains.builder()
      .withValue(value)
      .withConsistency(consistency)
      .build());
  }

  /**
   * Gets the queue count.
   *
   * @return A completable future to be completed with the queue count.
   */
  public CompletableFuture<Integer> size() {
    return submit(QueueCommands.Size.builder().build());
  }

  /**
   * Gets the queue count.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the queue count.
   */
  public CompletableFuture<Integer> size(Query.ConsistencyLevel consistency) {
    return submit(QueueCommands.Size.builder().withConsistency(consistency).build());
  }

  /**
   * Checks whether the queue is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the queue is empty.
   */
  public CompletableFuture<Boolean> isEmpty() {
    return submit(QueueCommands.IsEmpty.builder().build());
  }

  /**
   * Checks whether the queue is empty.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with a boolean value indicating whether the queue is empty.
   */
  public CompletableFuture<Boolean> isEmpty(Query.ConsistencyLevel consistency) {
    return submit(QueueCommands.IsEmpty.builder().withConsistency(consistency).build());
  }

  /**
   * Removes all values from the queue.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear() {
    return submit(QueueCommands.Clear.builder().build());
  }

  /**
   * Removes all values from the queue.
   *
   * @param consistency The command consistency level.
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear(Command.ConsistencyLevel consistency) {
    return submit(QueueCommands.Clear.builder()
      .withConsistency(consistency)
      .build());
  }

}
