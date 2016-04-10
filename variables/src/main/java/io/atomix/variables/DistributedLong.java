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
package io.atomix.variables;

import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.ReadConsistency;
import io.atomix.resource.ResourceTypeInfo;
import io.atomix.variables.internal.LongCommands;
import io.atomix.variables.util.DistributedLongFactory;

import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Stores a replicated 64-bit number, supporting atomic increments, decrements, and compare-and-set.
 * <p>
 * The {@code DistributedLong} resource is modeled on Java's {@link java.util.concurrent.atomic.AtomicLong},
 * providing asynchronous versions of all the methods present in that class. Users can use {@code DistributedLong}
 * to generate unique IDs or monotonically increasing tokens or as a counter. State change operations are
 * linearizable and are therefore guaranteed to take place some time between the invocation of a state change
 * method and the completion of the returned {@link CompletableFuture}.
 * <pre>
 *   {@code
 *   DistributedLong value = atomix.getLong("foo").get();
 *   value.set(1).join();
 *   value.incrementAndGet(value -> {
 *     ...
 *   });
 *   }
 * </pre>
 * <h3>Implementation</h3>
 * State management for the {@code DistributedLong} resource is implemented as a snapshottable Copycat
 * {@link io.atomix.copycat.server.StateMachine}. Changes to the long value are written to a log and
 * replicated to a majority of the cluster before being applied to the state machine. State changes
 * are applied to the state machine atomically, and the value is stored in a single 64-bit field.
 * Periodically, a 64-bit snapshot of the resource's state is written to disk, and prior
 * {@link #incrementAndGet() increment} and {@link #decrementAndGet() decrement} operations are removed
 * from the replicated log during compaction.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@ResourceTypeInfo(id=-2, factory=DistributedLongFactory.class)
public class DistributedLong extends AbstractDistributedValue<DistributedLong, Long> {

  public DistributedLong(CopycatClient client, Properties options) {
    super(client, options);
  }

  /**
   * Adds a delta to the long and returns the updated value.
   * <p>
   * This is an atomic operation that is guaranteed to take place exactly once between the invocation
   * of the method call and the completion of the returned {@link CompletableFuture}. Once the returned
   * {@link CompletableFuture} is completed, all other instances of the resource can see the state change
   * when writing or reading using {@link ReadConsistency#ATOMIC atomic consistency}.
   * <p>
   * If the operation is successful, the returned future will be completed with the updated value.
   *
   * @param delta The delta to add.
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> addAndGet(long delta) {
    return submit(new LongCommands.AddAndGet(delta));
  }

  /**
   * Adds a delta to the value and returns the previous value.
   * <p>
   * This is an atomic operation that is guaranteed to take place exactly once between the invocation
   * of the method call and the completion of the returned {@link CompletableFuture}. Once the returned
   * {@link CompletableFuture} is completed, all other instances of the resource can see the state change
   * when writing or reading using {@link ReadConsistency#ATOMIC atomic consistency}.
   * <p>
   * If the operation is successful, the returned future will be completed with value prior to the update.
   *
   * @param delta The delta to add.
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> getAndAdd(long delta) {
    return submit(new LongCommands.GetAndAdd(delta));
  }

  /**
   * Increments the value and returns the updated value.
   * <p>
   * This is an atomic operation that is guaranteed to take place exactly once between the invocation
   * of the method call and the completion of the returned {@link CompletableFuture}. Once the returned
   * {@link CompletableFuture} is completed, all other instances of the resource can see the state change
   * when writing or reading using {@link ReadConsistency#ATOMIC atomic consistency}.
   * <p>
   * If the operation is successful, the returned future will be completed with the updated value.
   *
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> incrementAndGet() {
    return submit(new LongCommands.IncrementAndGet());
  }

  /**
   * Decrements the value and returns the updated value.
   * <p>
   * This is an atomic operation that is guaranteed to take place exactly once between the invocation
   * of the method call and the completion of the returned {@link CompletableFuture}. Once the returned
   * {@link CompletableFuture} is completed, all other instances of the resource can see the state change
   * when writing or reading using {@link ReadConsistency#ATOMIC atomic consistency}.
   * <p>
   * If the operation is successful, the returned future will be completed with the updated value.
   *
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> decrementAndGet() {
    return submit(new LongCommands.DecrementAndGet());
  }

  /**
   * Increments the value and returns the previous value.
   * <p>
   * This is an atomic operation that is guaranteed to take place exactly once between the invocation
   * of the method call and the completion of the returned {@link CompletableFuture}. Once the returned
   * {@link CompletableFuture} is completed, all other instances of the resource can see the state change
   * when writing or reading using {@link ReadConsistency#ATOMIC atomic consistency}.
   * <p>
   * If the operation is successful, the returned future will be completed with the value prior to the update.
   *
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> getAndIncrement() {
    return submit(new LongCommands.GetAndIncrement());
  }

  /**
   * Decrements the value and returns the previous value.
   * <p>
   * This is an atomic operation that is guaranteed to take place exactly once between the invocation
   * of the method call and the completion of the returned {@link CompletableFuture}. Once the returned
   * {@link CompletableFuture} is completed, all other instances of the resource can see the state change
   * when writing or reading using {@link ReadConsistency#ATOMIC atomic consistency}.
   * <p>
   * If the operation is successful, the returned future will be completed with the value prior to the update.
   *
   * @return A completable future to be completed with the result.
   */
  public CompletableFuture<Long> getAndDecrement() {
    return submit(new LongCommands.GetAndDecrement());
  }

}
