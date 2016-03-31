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

import io.atomix.collections.internal.SetCommands;
import io.atomix.collections.util.DistributedSetFactory;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.ReadConsistency;
import io.atomix.resource.ResourceTypeInfo;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed collection of unique values.
 * <p>
 * The distributed set is closely modeled on Java's {@link java.util.HashSet}. All methods present
 * in the {@link java.util.Set} interface are present in this interface. Set items are held in
 * memory on each stateful node and backed by replicated logs on disk, thus the size of a set
 * is limited by the memory available to the smallest node in the cluster. Internally, {@code DistributedSet}
 * uses a {@link java.util.HashMap} to store set items in memory in the replicated state machine.
 * <p>
 * To create a distributed set, use the {@code getSet} factory method:
 * <pre>
 *   {@code
 *   DistributedSet<String> set = atomix.getSet("foo").get();
 *   }
 * </pre>
 * All set modification operations are linearizable, so items added to or removed from the set will
 * be immediately reflected from the perspective of all clients operating on the set. The set is
 * shared by processes based on the set name.
 * <p>
 * Sets support relaxed consistency levels for some read operations line {@link #size(ReadConsistency)}
 * and {@link #contains(Object, ReadConsistency)}. By default, read operations on a set are linearizable
 * but require some level of communication between nodes.
 *
 * @param <T> The set value type.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-13, factory=DistributedSetFactory.class)
public class DistributedSet<T> extends AbstractResource<DistributedSet<T>> {
  public DistributedSet(CopycatClient client, Properties options) {
    super(client, options);
  }

  /**
   * Adds a value to the set.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> add(T value) {
    return submit(new SetCommands.Add(value));
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
    return submit(new SetCommands.Add(value, ttl.toMillis()));
  }

  /**
   * Removes a value from the set.
   *
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(T value) {
    return submit(new SetCommands.Remove(value));
  }

  /**
   * Checks whether the set contains a value.
   *
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> contains(Object value) {
    return submit(new SetCommands.Contains(value));
  }

  /**
   * Checks whether the set contains a value.
   *
   * @param value The value to check.
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> contains(Object value, ReadConsistency consistency) {
    return submit(new SetCommands.Contains(value), consistency);
  }

  /**
   * Gets the set count.
   *
   * @return A completable future to be completed with the set count.
   */
  public CompletableFuture<Integer> size() {
    return submit(new SetCommands.Size());
  }

  /**
   * Gets the set count.
   *
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the set count.
   */
  public CompletableFuture<Integer> size(ReadConsistency consistency) {
    return submit(new SetCommands.Size(), consistency);
  }

  /**
   * Checks whether the set is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the set is empty.
   */
  public CompletableFuture<Boolean> isEmpty() {
    return submit(new SetCommands.IsEmpty());
  }

  /**
   * Checks whether the set is empty.
   *
   * @param consistency The read consistency level.
   * @return A completable future to be completed with a boolean value indicating whether the set is empty.
   */
  public CompletableFuture<Boolean> isEmpty(ReadConsistency consistency) {
    return submit(new SetCommands.IsEmpty(), consistency);
  }

  /**
   * Removes all values from the set.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear() {
    return submit(new SetCommands.Clear());
  }

}
