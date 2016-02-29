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

import io.atomix.collections.state.QueueCommands;
import io.atomix.collections.state.QueueState;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.ReadConsistency;
import io.atomix.resource.Resource;
import io.atomix.resource.ResourceTypeInfo;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed collection designed for holding ordered items for processing.
 * <p>
 * The distributed queue is closely modeled on Java's queues. All methods present in the
 * {@link java.util.Queue} interface are present in this interface. Queued items are held in
 * memory on each stateful node and backed by replicated logs on disk, thus the size of a queue
 * is limited by the memory available to the smallest node in the cluster.
 * <p>
 * Internally, {@code DistributedQueue} uses an {@link java.util.ArrayDeque} to enqueue items
 * in memory in the replicated state machine. Operations submitted through this interface are
 * replicated and result in calling the associated method on the replicated {@link java.util.ArrayDeque}
 * on each stateful node.
 * <p>
 * To create a distributed queue, use the {@code getQueue} factory method:
 * <pre>
 *   {@code
 *   DistributedQueue<String> queue = atomix.getQueue("foo").get();
 *   }
 * </pre>
 * All queue modification operations are linearizable, so items added to or removed from the queue will
 * be immediately reflected from the perspective of all clients operating on the queue. The queue is
 * shared by processes based on the queue name.
 * <p>
 * Queues support relaxed consistency levels for some read operations line {@link #size(ReadConsistency)}
 * and {@link #contains(Object, ReadConsistency)}. By default, read operations on a queue are linearizable
 * but require some level of communication between nodes.
 *
 * @param <T> The queue value type.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-14, stateMachine=QueueState.class, typeResolver=QueueCommands.TypeResolver.class)
public class DistributedQueue<T> extends Resource<DistributedQueue<T>> {

  public DistributedQueue(CopycatClient client, Properties config, Properties options) {
    super(client, config, options);
  }

  /**
   * Adds a value to the set.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> add(T value) {
    return submit(new QueueCommands.Add(value));
  }

  /**
   * Adds a value to the queue.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> offer(T value) {
    return submit(new QueueCommands.Offer(value));
  }

  /**
   * Removes a value from the queue.
   *
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> peek() {
    return submit(new QueueCommands.Peek()).thenApply(v -> (T) v);
  }

  /**
   * Removes a value from the queue.
   *
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> poll() {
    return submit(new QueueCommands.Poll()).thenApply(v -> (T) v);
  }

  /**
   * Removes a value from the queue.
   *
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> element() {
    return submit(new QueueCommands.Element()).thenApply(v -> (T) v);
  }

  /**
   * Removes a value from the queue.
   *
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> remove() {
    return submit(new QueueCommands.Remove()).thenApply(v -> (T) v);
  }

  /**
   * Removes a value from the set.
   *
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(T value) {
    return submit(new QueueCommands.Remove(value)).thenApply(v -> (boolean) v);
  }

  /**
   * Checks whether the set contains a value.
   *
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> contains(Object value) {
    return submit(new QueueCommands.Contains(value));
  }

  /**
   * Checks whether the set contains a value.
   *
   * @param value The value to check.
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> contains(Object value, ReadConsistency consistency) {
    return submit(new QueueCommands.Contains(value), consistency);
  }

  /**
   * Gets the set count.
   *
   * @return A completable future to be completed with the set count.
   */
  public CompletableFuture<Integer> size() {
    return submit(new QueueCommands.Size());
  }

  /**
   * Gets the set count.
   *
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the set count.
   */
  public CompletableFuture<Integer> size(ReadConsistency consistency) {
    return submit(new QueueCommands.Size(), consistency);
  }

  /**
   * Checks whether the set is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the set is empty.
   */
  public CompletableFuture<Boolean> isEmpty() {
    return submit(new QueueCommands.IsEmpty());
  }

  /**
   * Checks whether the set is empty.
   *
   * @param consistency The read consistency level.
   * @return A completable future to be completed with a boolean value indicating whether the set is empty.
   */
  public CompletableFuture<Boolean> isEmpty(ReadConsistency consistency) {
    return submit(new QueueCommands.IsEmpty(), consistency);
  }

  /**
   * Removes all values from the set.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear() {
    return submit(new QueueCommands.Clear());
  }

}
