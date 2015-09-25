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
package io.atomix.copycat.atomic;

import io.atomix.catalogue.client.Command;
import io.atomix.catalogue.client.Query;
import io.atomix.catalogue.server.StateMachine;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.copycat.Resource;
import io.atomix.copycat.atomic.state.AtomicValueCommands;
import io.atomix.copycat.atomic.state.AtomicValueState;
import io.atomix.copycat.resource.ResourceContext;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Distributed atomic value.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DistributedAtomicValue<T> extends Resource {
  private Command.ConsistencyLevel commandConsistency = Command.ConsistencyLevel.LINEARIZABLE;
  private Query.ConsistencyLevel queryConsistency = Query.ConsistencyLevel.LINEARIZABLE;
  private final java.util.Set<Consumer<T>> changeListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return AtomicValueState.class;
  }

  @Override
  protected void open(ResourceContext context) {
    super.open(context);
    context.session().<T>onEvent("change", event -> {
      for (Consumer<T> listener : changeListeners) {
        listener.accept(event);
      }
    });
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
  public DistributedAtomicValue<T> withDefaultCommandConsistency(Command.ConsistencyLevel consistency) {
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
  public DistributedAtomicValue<T> withDefaultQueryConsistency(Query.ConsistencyLevel consistency) {
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
   * Gets the current value.
   *
   * @return A completable future to be completed with the current value.
   */
  public CompletableFuture<T> get() {
    return get(queryConsistency);
  }

  /**
   * Gets the current value.
   *
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the current value.
   */
  public CompletableFuture<T> get(Query.ConsistencyLevel consistency) {
    return submit(AtomicValueCommands.Get.<T>builder()
      .withConsistency(consistency)
      .build());
  }

  /**
   * Sets the current value.
   *
   * @param value The current value.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value) {
    return set(value, commandConsistency);
  }

  /**
   * Sets the value with a TTL.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value, Duration ttl) {
    return set(value, ttl, commandConsistency);
  }

  /**
   * Sets the current value.
   *
   * @param value The current value.
   * @param consistency The write consistency level.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value, Command.ConsistencyLevel consistency) {
    return submit(AtomicValueCommands.Set.builder()
      .withValue(value)
      .withConsistency(consistency)
      .build());
  }

  /**
   * Sets the value with a write persistence.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @param consistency The write consistency level.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value, Duration ttl, Command.ConsistencyLevel consistency) {
    return submit(AtomicValueCommands.Set.builder()
      .withValue(value)
      .withTtl(ttl.toMillis())
      .withConsistency(consistency)
      .build());
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value) {
    return getAndSet(value, commandConsistency);
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value, Duration ttl) {
    return getAndSet(value, ttl, commandConsistency);
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param consistency The write consistency level.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value, Command.ConsistencyLevel consistency) {
    return submit(AtomicValueCommands.GetAndSet.<T>builder()
      .withValue(value)
      .withConsistency(consistency)
      .build());
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @param consistency The write consistency level.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value, Duration ttl, Command.ConsistencyLevel consistency) {
    return submit(AtomicValueCommands.GetAndSet.<T>builder()
      .withValue(value)
      .withTtl(ttl.toMillis())
      .withConsistency(consistency)
      .build());
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(T expect, T update) {
    return compareAndSet(expect, update, commandConsistency);
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, Duration ttl) {
    return compareAndSet(expect, update, ttl, commandConsistency);
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param consistency The write consistency level.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, Command.ConsistencyLevel consistency) {
    return submit(AtomicValueCommands.CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withConsistency(consistency)
      .build());
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param ttl The time after which to expire the value.
   * @param consistency The write consistency level.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, Duration ttl, Command.ConsistencyLevel consistency) {
    return submit(AtomicValueCommands.CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withTtl(ttl.toMillis())
      .withConsistency(consistency)
      .build());
  }

  /**
   * Registers a change listener.
   *
   * @param listener The change listener.
   * @return A completable future to be completed once the change listener has been registered.
   */
  public synchronized CompletableFuture<Listener<T>> onChange(Consumer<T> listener) {
    if (!changeListeners.isEmpty()) {
      changeListeners.add(listener);
      return CompletableFuture.completedFuture(new ChangeListener(listener));
    }

    changeListeners.add(listener);
    return submit(AtomicValueCommands.Listen.builder().build())
      .thenApply(v -> new ChangeListener(listener));
  }

  /**
   * Change listener context.
   */
  private class ChangeListener implements Listener<T> {
    private final Consumer<T> listener;

    private ChangeListener(Consumer<T> listener) {
      this.listener = listener;
    }

    @Override
    public void accept(T event) {
      listener.accept(event);
    }

    @Override
    public void close() {
      synchronized (DistributedAtomicValue.this) {
        changeListeners.remove(listener);
        if (changeListeners.isEmpty()) {
          submit(AtomicValueCommands.Unlisten.builder().build());
        }
      }
    }
  }

}
