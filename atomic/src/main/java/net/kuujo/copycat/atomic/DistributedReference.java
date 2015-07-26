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

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.AlleycatSerializable;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.copycat.*;
import net.kuujo.copycat.log.Compaction;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.server.Apply;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.Filter;

import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Asynchronous atomic value.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Stateful(DistributedReference.StateMachine.class)
public class DistributedReference<T> extends Resource {
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
  public DistributedReference<T> withDefaultConsistencyLevel(ConsistencyLevel consistency) {
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
    return submit(Get.<T>builder()
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
    return submit(Set.builder()
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
  public CompletableFuture<Void> set(T value, long ttl) {
    return submit(Set.builder()
      .withValue(value)
      .withTtl(ttl)
      .build());
  }

  /**
   * Sets the value with a TTL.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @param unit The expiration time unit.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value, long ttl, TimeUnit unit) {
    return submit(Set.builder()
      .withValue(value)
      .withTtl(ttl, unit)
      .build());
  }

  /**
   * Sets the value with a write mode.
   *
   * @param value The value to set.
   * @param mode The write mode.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value, Mode mode) {
    return submit(Set.builder()
      .withValue(value)
      .withMode(mode)
      .build());
  }

  /**
   * Sets the value with a write mode.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @param mode The write mode.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value, long ttl, Mode mode) {
    return submit(Set.builder()
      .withValue(value)
      .withTtl(ttl)
      .withMode(mode)
      .build());
  }

  /**
   * Sets the value with a write mode.
   *
   * @param value The value to set.
   * @param ttl The time after which to expire the value.
   * @param unit The expiration time unit.
   * @param mode The write mode.
   * @return A completable future to be completed once the value has been set.
   */
  public CompletableFuture<Void> set(T value, long ttl, TimeUnit unit, Mode mode) {
    return submit(Set.builder()
      .withValue(value)
      .withTtl(ttl, unit)
      .withMode(mode)
      .build());
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value) {
    return submit(GetAndSet.<T>builder()
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
  public CompletableFuture<T> getAndSet(T value, long ttl) {
    return submit(GetAndSet.<T>builder()
      .withValue(value)
      .withTtl(ttl)
      .build());
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @param unit The expiration time unit.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value, long ttl, TimeUnit unit) {
    return submit(GetAndSet.<T>builder()
      .withValue(value)
      .withTtl(ttl, unit)
      .build());
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param mode The write mode.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value, Mode mode) {
    return submit(GetAndSet.<T>builder()
      .withValue(value)
      .withMode(mode)
      .build());
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @param mode The write mode.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value, long ttl, Mode mode) {
    return submit(GetAndSet.<T>builder()
      .withValue(value)
      .withTtl(ttl)
      .withMode(mode)
      .build());
  }

  /**
   * Gets the current value and updates it.
   *
   * @param value The updated value.
   * @param ttl The time after which to expire the value.
   * @param unit The expiration time unit.
   * @param mode The write mode.
   * @return A completable future to be completed with the previous value.
   */
  public CompletableFuture<T> getAndSet(T value, long ttl, TimeUnit unit, Mode mode) {
    return submit(GetAndSet.<T>builder()
      .withValue(value)
      .withTtl(ttl, unit)
      .withMode(mode)
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
    return submit(CompareAndSet.builder()
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
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, long ttl) {
    return submit(CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withTtl(ttl)
      .build());
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param ttl The time after which to expire the value.
   * @param unit The expiration time unit.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, long ttl, TimeUnit unit) {
    return submit(CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withTtl(ttl, unit)
      .build());
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param mode The write mode.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, Mode mode) {
    return submit(CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withMode(mode)
      .build());
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param ttl The time after which to expire the value.
   * @param mode The write mode.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, long ttl, Mode mode) {
    return submit(CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withTtl(ttl)
      .withMode(mode)
      .build());
  }

  /**
   * Compares the current value and updated it if expected value == the current value.
   *
   * @param expect The expected value.
   * @param update The updated value.
   * @param ttl The time after which to expire the value.
   * @param unit The expiration time unit.
   * @param mode The write mode.
   * @return A completable future to be completed with a boolean value indicating whether the value was updated.
   */
  public CompletableFuture<Boolean> compareAndSet(T expect, T update, long ttl, TimeUnit unit, Mode mode) {
    return submit(CompareAndSet.builder()
      .withExpect(expect)
      .withUpdate(update)
      .withTtl(ttl, unit)
      .withMode(mode)
      .build());
  }

  /**
   * Registers a change listener.
   *
   * @param listener The change listener.
   * @return A completable future to be completed once the change listener has been registered.
   */
  public synchronized CompletableFuture<ListenerContext<T>> onChange(Listener<T> listener) {
    if (!changeListeners.isEmpty()) {
      changeListeners.add(listener);
      return CompletableFuture.completedFuture(new ChangeListenerContext(listener));
    }

    changeListeners.add(listener);
    return submit(ChangeListen.builder().build())
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
          submit(ChangeUnlisten.builder().build());
        }
      }
    }
  }

  /**
   * Abstract reference command.
   */
  public static abstract class ReferenceCommand<V> implements Command<V>, AlleycatSerializable {
    protected Mode mode = Mode.PERSISTENT;
    protected long ttl;

    /**
     * Returns the persistence mode.
     *
     * @return The persistence mode.
     */
    public Mode mode() {
      return mode;
    }

    /**
     * Returns the time to live in milliseconds.
     *
     * @return The time to live in milliseconds.
     */
    public long ttl() {
      return ttl;
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
      buffer.writeByte(mode.ordinal())
        .writeLong(ttl);
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
      mode = Mode.values()[buffer.readByte()];
      ttl = buffer.readLong();
    }

    /**
     * Base reference command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends ReferenceCommand<V>, V> extends Command.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }

      /**
       * Sets the persistence mode.
       *
       * @param mode The persistence mode.
       * @return The command builder.
       */
      @SuppressWarnings("unchecked")
      public T withMode(Mode mode) {
        if (mode == null)
          throw new NullPointerException("mode cannot be null");
        command.mode = mode;
        return (T) this;
      }

      /**
       * Sets the time to live.
       *
       * @param ttl The time to live in milliseconds..
       * @return The command builder.
       */
      @SuppressWarnings("unchecked")
      public T withTtl(long ttl) {
        command.ttl = ttl;
        return (T) this;
      }

      /**
       * Sets the time to live.
       *
       * @param ttl The time to live.
       * @param unit The time to live unit.
       * @return The command builder.
       */
      @SuppressWarnings("unchecked")
      public T withTtl(long ttl, TimeUnit unit) {
        command.ttl = unit.toMillis(ttl);
        return (T) this;
      }
    }
  }

  /**
   * Abstract reference query.
   */
  public static abstract class ReferenceQuery<V> implements Query<V>, AlleycatSerializable {
    protected ConsistencyLevel consistency = ConsistencyLevel.LINEARIZABLE_LEASE;

    @Override
    public ConsistencyLevel consistency() {
      return consistency;
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
      buffer.writeByte(consistency.ordinal());
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
      consistency = ConsistencyLevel.values()[buffer.readByte()];
    }

    /**
     * Base reference query builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends ReferenceQuery<V>, V> extends Query.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }

      /**
       * Sets the query consistency level.
       *
       * @param consistency The query consistency level.
       * @return The query builder.
       */
      @SuppressWarnings("unchecked")
      public T withConsistency(ConsistencyLevel consistency) {
        if (consistency == null)
          throw new NullPointerException("consistency cannot be null");
        query.consistency = consistency;
        return (T) this;
      }
    }
  }

  /**
   * Get query.
   */
  @SerializeWith(id=460)
  public static class Get<T> extends ReferenceQuery<T> {

    /**
     * Returns a new get query builder.
     *
     * @return A new get query builder.
     */
    @SuppressWarnings("unchecked")
    public static <T> Builder<T> builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Get query builder.
     */
    public static class Builder<T> extends ReferenceQuery.Builder<Builder<T>, Get<T>, T> {
      public Builder(BuilderPool<Builder<T>, Get<T>> pool) {
        super(pool);
      }

      @Override
      protected Get<T> create() {
        return new Get<>();
      }
    }
  }

  /**
   * Set command.
   */
  @SerializeWith(id=461)
  public static class Set extends ReferenceCommand<Void> {

    /**
     * Returns a new set command builder.
     *
     * @return A new set command builder.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    private Object value;

    /**
     * Returns the command value.
     *
     * @return The command value.
     */
    public Object value() {
      return value;
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
      alleycat.writeObject(value, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
      value = alleycat.readObject(buffer);
    }

    @Override
    public String toString() {
      return String.format("%s[value=%s]", getClass().getSimpleName(), value);
    }

    /**
     * Put command builder.
     */
    public static class Builder extends ReferenceCommand.Builder<Builder, Set, Void> {
      public Builder(BuilderPool<Builder, Set> pool) {
        super(pool);
      }

      @Override
      protected Set create() {
        return new Set();
      }

      /**
       * Sets the command value.
       *
       * @param value The command value.
       * @return The command builder.
       */
      public Builder withValue(Object value) {
        command.value = value;
        return this;
      }
    }
  }

  /**
   * Compare and set command.
   */
  @SerializeWith(id=462)
  public static class CompareAndSet extends ReferenceCommand<Boolean> {

    /**
     * Returns a new compare and set command builder.
     *
     * @return A new compare and set command builder.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    private Object expect;
    private Object update;

    /**
     * Returns the expected value.
     *
     * @return The expected value.
     */
    public Object expect() {
      return expect;
    }

    /**
     * Returns the updated value.
     *
     * @return The updated value.
     */
    public Object update() {
      return update;
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
      alleycat.writeObject(expect, buffer);
      alleycat.writeObject(update, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
      expect = alleycat.readObject(buffer);
      update = alleycat.readObject(buffer);
    }

    @Override
    public String toString() {
      return String.format("%s[expect=%s, update=%s]", getClass().getSimpleName(), expect, update);
    }

    /**
     * Compare and set command builder.
     */
    public static class Builder extends ReferenceCommand.Builder<Builder, CompareAndSet, Boolean> {
      public Builder(BuilderPool<Builder, CompareAndSet> pool) {
        super(pool);
      }

      @Override
      protected CompareAndSet create() {
        return new CompareAndSet();
      }

      /**
       * Sets the expected value.
       *
       * @param expect The expected value.
       * @return The command builder.
       */
      public Builder withExpect(Object expect) {
        command.expect = expect;
        return this;
      }

      /**
       * Sets the updated value.
       *
       * @param update The updated value.
       * @return The command builder.
       */
      public Builder withUpdate(Object update) {
        command.update = update;
        return this;
      }
    }
  }

  /**
   * Get and set command.
   */
  @SerializeWith(id=463)
  public static class GetAndSet<T> extends ReferenceCommand<T> {

    /**
     * Returns a new get and set command builder.
     *
     * @return A new get and set command builder.
     */
    @SuppressWarnings("unchecked")
    public static <T> Builder<T> builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    private Object value;

    /**
     * Returns the command value.
     *
     * @return The command value.
     */
    public Object value() {
      return value;
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
      alleycat.writeObject(value, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
      value = alleycat.readObject(buffer);
    }

    @Override
    public String toString() {
      return String.format("%s[value=%s]", getClass().getSimpleName(), value);
    }

    /**
     * Put command builder.
     */
    public static class Builder<T> extends ReferenceCommand.Builder<Builder<T>, GetAndSet<T>, T> {
      public Builder(BuilderPool<Builder<T>, GetAndSet<T>> pool) {
        super(pool);
      }

      @Override
      protected GetAndSet<T> create() {
        return new GetAndSet<>();
      }

      /**
       * Sets the command value.
       *
       * @param value The command value.
       * @return The command builder.
       */
      public Builder<T> withValue(Object value) {
        command.value = value;
        return this;
      }
    }
  }

  /**
   * Change listen.
   */
  @SerializeWith(id=464)
  public static class ChangeListen implements Command<Void>, AlleycatSerializable {

    /**
     * Returns a new change listen builder.
     *
     * @return A new change listen builder.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {

    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {

    }

    /**
     * Change listen builder.
     */
    public static class Builder extends Command.Builder<Builder, ChangeListen, Void> {
      public Builder(BuilderPool<Builder, ChangeListen> pool) {
        super(pool);
      }

      @Override
      protected ChangeListen create() {
        return new ChangeListen();
      }
    }
  }

  /**
   * Change unlisten.
   */
  @SerializeWith(id=465)
  public static class ChangeUnlisten implements Command<Void>, AlleycatSerializable {

    /**
     * Returns a new change unlisten builder.
     *
     * @return A new change unlisten builder.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {

    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {

    }

    /**
     * Change unlisten builder.
     */
    public static class Builder extends Command.Builder<Builder, ChangeUnlisten, Void> {
      public Builder(BuilderPool<Builder, ChangeUnlisten> pool) {
        super(pool);
      }

      @Override
      protected ChangeUnlisten create() {
        return new ChangeUnlisten();
      }
    }
  }

  /**
   * Async reference state machine.
   */
  public static class StateMachine extends net.kuujo.copycat.raft.server.StateMachine {
    private final java.util.Set<Long> sessions = new HashSet<>();
    private final java.util.Set<Session> listeners = new HashSet<>();
    private final AtomicReference<Object> value = new AtomicReference<>();
    private Commit<? extends ReferenceCommand> command;
    private long version;
    private long time;

    /**
     * Updates the state machine timestamp.
     */
    private void updateTime(Commit commit) {
      this.time = Math.max(time, commit.timestamp());
    }

    @Override
    public void register(Session session) {
      sessions.add(session.id());
    }

    @Override
    public void expire(Session session) {
      sessions.remove(session.id());
    }

    @Override
    public void close(Session session) {
      sessions.remove(session.id());
    }

    /**
     * Returns a boolean value indicating whether the given commit is active.
     */
    private boolean isActive(Commit<? extends ReferenceCommand> commit) {
      if (commit == null) {
        return false;
      } else if (commit.operation().mode() == Mode.EPHEMERAL && !sessions.contains(commit.session().id())) {
        return false;
      } else if (commit.operation().ttl() != 0 && commit.operation().ttl() < time - commit.timestamp()) {
        return false;
      }
      return true;
    }

    /**
     * Handles a listen commit.
     */
    @Apply(ChangeListen.class)
    protected void listen(Commit<ChangeListen> commit) {
      updateTime(commit);
      listeners.add(commit.session());
    }

    /**
     * Handles an unlisten commit.
     */
    @Apply(ChangeUnlisten.class)
    protected void unlisten(Commit<ChangeUnlisten> commit) {
      updateTime(commit);
      listeners.remove(commit.session());
    }

    /**
     * Triggers a change event.
     */
    private void change(Object value) {
      for (Session session : listeners) {
        session.publish(value);
      }
    }

    /**
     * Handles a get commit.
     */
    @Apply(Get.class)
    protected Object get(Commit<Get> commit) {
      updateTime(commit);
      return value.get();
    }

    /**
     * Applies a set commit.
     */
    @Apply(Set.class)
    protected void set(Commit<Set> commit) {
      updateTime(commit);
      value.set(commit.operation().value());
      command = commit;
      version = commit.index();
      change(value.get());
    }

    /**
     * Handles a compare and set commit.
     */
    @Apply(CompareAndSet.class)
    protected boolean compareAndSet(Commit<CompareAndSet> commit) {
      updateTime(commit);
      if (isActive(command)) {
        if (value.compareAndSet(commit.operation().expect(), commit.operation().update())) {
          command = commit;
          change(value.get());
          return true;
        }
        return false;
      } else if (commit.operation().expect() == null) {
        value.set(null);
        command = commit;
        version = commit.index();
        change(null);
        return true;
      } else {
        return false;
      }
    }

    /**
     * Handles a get and set commit.
     */
    @Apply(GetAndSet.class)
    protected Object getAndSet(Commit<GetAndSet> commit) {
      updateTime(commit);
      if (isActive(command)) {
        Object result = value.getAndSet(commit.operation().value());
        command = commit;
        version = commit.index();
        change(value.get());
        return result;
      } else {
        value.set(commit.operation().value());
        command = commit;
        version = commit.index();
        change(value.get());
        return null;
      }
    }

    /**
     * Filters all entries.
     */
    @Filter(Filter.All.class)
    protected boolean filterAll(Commit<? extends ReferenceCommand<?>> commit, Compaction compaction) {
      return commit.index() >= version && isActive(commit);
    }
  }

}
