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
package io.atomix.atomic;

import io.atomix.atomic.state.AtomicValueCommands;
import io.atomix.atomic.state.AtomicValueState;
import io.atomix.catalyst.util.Listener;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.resource.AbstractResource;
import io.atomix.resource.Consistency;
import io.atomix.resource.ResourceInfo;

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
@ResourceInfo(stateMachine=AtomicValueState.class)
public class DistributedAtomicValue<T> extends AbstractResource {
  private final java.util.Set<Consumer<T>> changeListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

  public DistributedAtomicValue(CopycatClient client) {
    super(client);
    client.session().<T>onEvent("change", event -> {
      for (Consumer<T> listener : changeListeners) {
        listener.accept(event);
      }
    });
  }

  @Override
  public DistributedAtomicValue<T> with(Consistency consistency) {
    super.with(consistency);
    return this;
  }

  /**
   * Gets the current value.
   *
   * @return A completable future to be completed with the current value.
   */
  public CompletableFuture<T> get() {
    return submit(new AtomicValueCommands.Get<>());
  }

  /**
   * Sets the current value.
   *
   * @param value The current value.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value) {
    return submit(new AtomicValueCommands.Set(value));
  }

  /**
   * Sets the value with a TTL.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value, Duration ttl) {
    return submit(new AtomicValueCommands.Set(value, ttl.toMillis()));
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value) {
    return submit(new AtomicValueCommands.GetAndSet<>(value));
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value, Duration ttl) {
    return submit(new AtomicValueCommands.GetAndSet<>(value, ttl.toMillis()));
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(T expect, T update) {
    return submit(new AtomicValueCommands.CompareAndSet(expect, update));
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
    return submit(new AtomicValueCommands.CompareAndSet(expect, update, ttl.toMillis()));
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
    return submit(new AtomicValueCommands.Listen())
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
          submit(new AtomicValueCommands.Unlisten());
        }
      }
    }
  }

}
