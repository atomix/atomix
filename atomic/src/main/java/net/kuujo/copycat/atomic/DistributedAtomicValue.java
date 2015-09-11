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
package net.kuujo.copycat.atomic;

import net.kuujo.copycat.PersistenceMode;
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.atomic.state.AtomicValueCommands;
import net.kuujo.copycat.atomic.state.AtomicValueState;
import net.kuujo.copycat.raft.StateMachine;
import net.kuujo.copycat.raft.protocol.ConsistencyLevel;
import net.kuujo.copycat.resource.ResourceContext;
import net.kuujo.copycat.util.Listener;

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
  private ConsistencyLevel defaultConsistency = ConsistencyLevel.LINEARIZABLE_LEASE;
  private final java.util.Set<Consumer<T>> changeListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

  @Override
  protected Class<? extends StateMachine> stateMachine() {
    return AtomicValueState.class;
  }

  @Override
  protected void open(ResourceContext context) {
    super.open(context);
    context.session().<T>onEvent(event -> {
      for (Consumer<T> listener : changeListeners) {
        listener.accept(event);
      }
    });
  }

  /**
   * Sets the default read consistency level.
   *
   * @param consistency The default read consistency level.
   * @throws java.lang.NullPointerException If the consistency level is {@code null}
   */
  public void setDefaultConsistencyLevel(ConsistencyLevel consistency) {
    if (consistency == null)
      throw new NullPointerException("consistency cannot be null");
    this.defaultConsistency = consistency;
  }

  /**
   * Sets the default consistency level, returning the resource for method chaining.
   *
   * @param consistency The default read consistency level.
   * @return The reference.
   * @throws java.lang.NullPointerException If the consistency level is {@code null}
   */
  public DistributedAtomicValue<T> withDefaultConsistencyLevel(ConsistencyLevel consistency) {
    setDefaultConsistencyLevel(consistency);
    return this;
  }

  /**
   * Returns the default consistency level.
   *
   * @return The default consistency level.
   */
  public ConsistencyLevel getDefaultConsistencyLevel() {
    return defaultConsistency;
  }

  /**
   * Gets the current value.
   *
   * @return A completable future to be completed with the current value.
   */
  public CompletableFuture<T> get() {
    return get(defaultConsistency);
  }

  /**
   * Gets the current value.
   *
   * @param consistency The read consistency level.
   * @return A completable future to be completed with the current value.
   */
  public CompletableFuture<T> get(ConsistencyLevel consistency) {
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
    return submit(AtomicValueCommands.Set.builder()
      .withValue(value)
      .build());
  }

  /**
   * Sets the value with a TTL.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value, Duration ttl) {
    return submit(AtomicValueCommands.Set.builder()
      .withValue(value)
      .withTtl(ttl.toMillis())
      .build());
  }

  /**
   * Sets the value with a write persistence.
   *
   * @param value The value to set.
   * @param persistence The write persistence.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value, PersistenceMode persistence) {
    return submit(AtomicValueCommands.Set.builder()
      .withValue(value)
      .withPersistence(persistence)
      .build());
  }

  /**
   * Sets the value with a write persistence.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @param persistence The write persistence.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value, Duration ttl, PersistenceMode persistence) {
    return submit(AtomicValueCommands.Set.builder()
      .withValue(value)
      .withTtl(ttl.toMillis())
      .withPersistence(persistence)
      .build());
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value) {
    return submit(AtomicValueCommands.GetAndSet.<T>builder()
      .withValue(value)
      .build());
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value, Duration ttl) {
    return submit(AtomicValueCommands.GetAndSet.<T>builder()
      .withValue(value)
      .withTtl(ttl.toMillis())
      .build());
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param persistence The write persistence.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value, PersistenceMode persistence) {
    return submit(AtomicValueCommands.GetAndSet.<T>builder()
      .withValue(value)
      .withPersistence(persistence)
      .build());
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @param persistence The write persistence.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value, Duration ttl, PersistenceMode persistence) {
    return submit(AtomicValueCommands.GetAndSet.<T>builder()
      .withValue(value)
      .withTtl(ttl.toMillis())
      .withPersistence(persistence)
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
    return submit(AtomicValueCommands.CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .build());
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
    return submit(AtomicValueCommands.CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withTtl(ttl.toMillis())
      .build());
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param persistence The write persistence.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, PersistenceMode persistence) {
    return submit(AtomicValueCommands.CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withPersistence(persistence)
      .build());
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param ttl The time after which to expire the value.
   * @param persistence The write persistence.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, Duration ttl, PersistenceMode persistence) {
    return submit(AtomicValueCommands.CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withTtl(ttl.toMillis())
      .withPersistence(persistence)
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
