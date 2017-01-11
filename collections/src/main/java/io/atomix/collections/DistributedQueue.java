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

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.collections.internal.QueueCommands;
import io.atomix.collections.util.DistributedQueueFactory;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.ReadConsistency;
import io.atomix.resource.ResourceTypeInfo;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

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
@ResourceTypeInfo(id=-14, factory=DistributedQueueFactory.class)
public class DistributedQueue<T> extends AbstractResource<DistributedQueue<T>> {

  public DistributedQueue(CopycatClient client, Properties options) {
    super(client, options);
  }

  /**
   * Adds a value to the set.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> add(T value) {
    return client.submit(new QueueCommands.Add(value));
  }

  /**
   * Adds a value to the queue.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> offer(T value) {
    return client.submit(new QueueCommands.Offer(value));
  }

  /**
   * Removes a value from the queue.
   *
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> peek() {
    return client.submit(new QueueCommands.Peek()).thenApply(v -> (T) v);
  }

  /**
   * Removes a value from the queue.
   *
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> poll() {
    return client.submit(new QueueCommands.Poll()).thenApply(v -> (T) v);
  }

  /**
   * Removes a value from the queue.
   *
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> element() {
    return client.submit(new QueueCommands.Element()).thenApply(v -> (T) v);
  }

  /**
   * Removes a value from the queue.
   *
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<T> remove() {
    return client.submit(new QueueCommands.Remove()).thenApply(v -> (T) v);
  }

  /**
   * Removes a value from the set.
   *
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(T value) {
    return client.submit(new QueueCommands.Remove(value)).thenApply(v -> (boolean) v);
  }

  /**
   * Checks whether the set contains a value.
   *
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> contains(Object value) {
    return client.submit(new QueueCommands.Contains(value));
  }

  /**
   * Checks whether the set contains a value.
   *
   * @param value The value to check.
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> contains(Object value, ReadConsistency consistency) {
    return client.submit(new QueueCommands.Contains(value, consistency.level()));
  }

  /**
   * Gets the set count.
   *
   * @return A completable future to be completed with the set count.
   */
  public CompletableFuture<Integer> size() {
    return client.submit(new QueueCommands.Size());
  }

  /**
   * Gets the set count.
   *
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the set count.
   */
  public CompletableFuture<Integer> size(ReadConsistency consistency) {
    return client.submit(new QueueCommands.Size(consistency.level()));
  }

  /**
   * Checks whether the set is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the set is empty.
   */
  public CompletableFuture<Boolean> isEmpty() {
    return client.submit(new QueueCommands.IsEmpty());
  }

  /**
   * Checks whether the set is empty.
   *
   * @param consistency The read consistency level.
   * @return A completable future to be completed with a boolean value indicating whether the set is empty.
   */
  public CompletableFuture<Boolean> isEmpty(ReadConsistency consistency) {
    return client.submit(new QueueCommands.IsEmpty(consistency.level()));
  }

  /**
   * Removes all values from the set.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear() {
    return client.submit(new QueueCommands.Clear());
  }

  /**
   * Registers a queue item add event listener.
   *
   * @param callback The event listener callback to be called when an item is added to the queue.
   * @return A completable future to be completed once the listener has been registered with the cluster.
   */
  public CompletableFuture<Listener<ValueEvent<T>>> onAdd(Consumer<ValueEvent<T>> callback) {
    return onEvent(Events.ADD, callback);
  }

  /**
   * Registers a queue item remove event listener.
   *
   * @param callback The event listener callback to be called when an item is removed from the quue.
   * @return A completable future to be completed once the listener has been registered with the cluster.
   */
  public CompletableFuture<Listener<ValueEvent<T>>> onRemove(Consumer<ValueEvent<T>> callback) {
    return onEvent(Events.REMOVE, callback);
  }

  /**
   * Distributed queue event types.
   */
  public enum Events implements EventType {
    /**
     * Queue add event.
     */
    ADD,

    /**
     * Queue remove event.
     */
    REMOVE;

    @Override
    public int id() {
      return ordinal();
    }
  }

  /**
   * Generic queue value event.
   */
  public static class ValueEvent<T> implements Event, CatalystSerializable {
    private EventType type;
    private T value;

    public ValueEvent() {
    }

    public ValueEvent(EventType type, T value) {
      this.type = type;
      this.value = value;
    }

    @Override
    public EventType type() {
      return type;
    }

    /**
     * Returns the event value.
     *
     * @return The event value.
     */
    public T value() {
      return value;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      buffer.writeByte(type.id());
      serializer.writeObject(value, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      type = Events.values()[buffer.readByte()];
      value = serializer.readObject(buffer);
    }
  }

}
