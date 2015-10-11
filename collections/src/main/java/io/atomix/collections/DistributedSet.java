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
import io.atomix.collections.state.SetCommands;
import io.atomix.collections.state.SetState;
import io.atomix.copycat.server.StateMachine;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Distributed set.
 *
 * @param <T> The set value type.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DistributedSet<T> extends DistributedResource<DistributedSet<T>> {

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
   * Gets the set count.
   *
   * @return A completable future to be completed with the set count.
   */
  public CompletableFuture<Integer> size() {
    return submit(new SetCommands.Size());
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
   * Removes all values from the set.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear() {
    return submit(new SetCommands.Clear());
  }

}
