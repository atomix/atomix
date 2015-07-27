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

import net.kuujo.copycat.*;
import net.kuujo.copycat.atomic.state.ReferenceCommands;
import net.kuujo.copycat.atomic.state.ReferenceState;
import net.kuujo.copycat.ConsistencyLevel;
import net.kuujo.copycat.Raft;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous atomic value.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Stateful(ReferenceState.class)
public class DistributedReference<T> extends Resource implements AsyncReference<T> {
  private ConsistencyLevel defaultConsistency = ConsistencyLevel.LINEARIZABLE_LEASE;
  private final java.util.Set<Listener<T>> changeListeners = Collections.newSetFromMap(new ConcurrentHashMap<>());

  public DistributedReference(Raft protocol) {
    super(protocol);
    protocol.session().<T>onReceive(event -> {
      for (Listener<T> listener : changeListeners) {
        listener.accept(event);
      }
    });
  }

  @Override
  public void setDefaultConsistencyLevel(ConsistencyLevel consistency) {
    if (consistency == null)
      throw new NullPointerException("consistency cannot be null");
    this.defaultConsistency = consistency;
  }

  @Override
  public DistributedReference<T> withDefaultConsistencyLevel(ConsistencyLevel consistency) {
    setDefaultConsistencyLevel(consistency);
    return this;
  }

  @Override
  public ConsistencyLevel getDefaultConsistencyLevel() {
    return defaultConsistency;
  }

  @Override
  public CompletableFuture<T> get() {
    return get(defaultConsistency);
  }

  @Override
  public CompletableFuture<T> get(ConsistencyLevel consistency) {
    return submit(ReferenceCommands.Get.<T>builder()
      .withConsistency(consistency)
      .build());
  }

  @Override
  public CompletableFuture<Void> set(T value) {
    return submit(ReferenceCommands.Set.builder()
      .withValue(value)
      .build());
  }

  @Override
  public CompletableFuture<Void> set(T value, long ttl) {
    return submit(ReferenceCommands.Set.builder()
      .withValue(value)
      .withTtl(ttl)
      .build());
  }

  @Override
  public CompletableFuture<Void> set(T value, long ttl, TimeUnit unit) {
    return submit(ReferenceCommands.Set.builder()
      .withValue(value)
      .withTtl(ttl, unit)
      .build());
  }

  @Override
  public CompletableFuture<Void> set(T value, PersistenceLevel persistence) {
    return submit(ReferenceCommands.Set.builder()
      .withValue(value)
      .withPersistence(persistence)
      .build());
  }

  @Override
  public CompletableFuture<Void> set(T value, long ttl, PersistenceLevel persistence) {
    return submit(ReferenceCommands.Set.builder()
      .withValue(value)
      .withTtl(ttl)
      .withPersistence(persistence)
      .build());
  }

  @Override
  public CompletableFuture<Void> set(T value, long ttl, TimeUnit unit, PersistenceLevel persistence) {
    return submit(ReferenceCommands.Set.builder()
      .withValue(value)
      .withTtl(ttl, unit)
      .withPersistence(persistence)
      .build());
  }

  @Override
  public CompletableFuture<T> getAndSet(T value) {
    return submit(ReferenceCommands.GetAndSet.<T>builder()
      .withValue(value)
      .build());
  }

  @Override
  public CompletableFuture<T> getAndSet(T value, long ttl) {
    return submit(ReferenceCommands.GetAndSet.<T>builder()
      .withValue(value)
      .withTtl(ttl)
      .build());
  }

  @Override
  public CompletableFuture<T> getAndSet(T value, long ttl, TimeUnit unit) {
    return submit(ReferenceCommands.GetAndSet.<T>builder()
      .withValue(value)
      .withTtl(ttl, unit)
      .build());
  }

  @Override
  public CompletableFuture<T> getAndSet(T value, PersistenceLevel persistence) {
    return submit(ReferenceCommands.GetAndSet.<T>builder()
      .withValue(value)
      .withPersistence(persistence)
      .build());
  }

  @Override
  public CompletableFuture<T> getAndSet(T value, long ttl, PersistenceLevel persistence) {
    return submit(ReferenceCommands.GetAndSet.<T>builder()
      .withValue(value)
      .withTtl(ttl)
      .withPersistence(persistence)
      .build());
  }

  @Override
  public CompletableFuture<T> getAndSet(T value, long ttl, TimeUnit unit, PersistenceLevel persistence) {
    return submit(ReferenceCommands.GetAndSet.<T>builder()
      .withValue(value)
      .withTtl(ttl, unit)
      .withPersistence(persistence)
      .build());
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(T expect, T update) {
    return submit(ReferenceCommands.CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .build());
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, long ttl) {
    return submit(ReferenceCommands.CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withTtl(ttl)
      .build());
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, long ttl, TimeUnit unit) {
    return submit(ReferenceCommands.CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withTtl(ttl, unit)
      .build());
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, PersistenceLevel persistence) {
    return submit(ReferenceCommands.CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withPersistence(persistence)
      .build());
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, long ttl, PersistenceLevel persistence) {
    return submit(ReferenceCommands.CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withTtl(ttl)
      .withPersistence(persistence)
      .build());
  }

  @Override
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, long ttl, TimeUnit unit, PersistenceLevel persistence) {
    return submit(ReferenceCommands.CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withTtl(ttl, unit)
      .withPersistence(persistence)
      .build());
  }

  @Override
  public synchronized CompletableFuture<ListenerContext<T>> onChange(Listener<T> listener) {
    if (!changeListeners.isEmpty()) {
      changeListeners.add(listener);
      return CompletableFuture.completedFuture(new ChangeListenerContext(listener));
    }

    changeListeners.add(listener);
    return submit(ReferenceCommands.Listen.builder().build())
      .thenApply(v -> new ChangeListenerContext(listener));
  }

  /**
   * Change listener context.
   */
  private class ChangeListenerContext implements ListenerContext<T> {
    private final Listener<T> listener;

    private ChangeListenerContext(Listener<T> listener) {
      this.listener = listener;
    }

    @Override
    public void accept(T event) {
      listener.accept(event);
    }

    @Override
    public void close() {
      synchronized (DistributedReference.this) {
        changeListeners.remove(listener);
        if (changeListeners.isEmpty()) {
          submit(ReferenceCommands.Unlisten.builder().build());
        }
      }
    }
  }

}
