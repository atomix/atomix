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
package net.kuujo.copycat.collections;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.serializer.Serializer;
import net.kuujo.copycat.io.serializer.Writable;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.log.compact.Compaction;
import net.kuujo.copycat.resource.AbstractResource;
import net.kuujo.copycat.resource.Stateful;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous set.
 *
 * @param <T> The set value type.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Stateful(AsyncSet.StateMachine.class)
public class AsyncSet<T> extends AbstractResource {

  public AsyncSet(Protocol protocol) {
    super(protocol);
  }

  /**
   * Adds a value to the set.
   *
   * @param value The value to add.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> add(T value) {
    return submit(Add.builder()
      .withValue(value)
      .build());
  }

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live in milliseconds.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, long ttl) {
    return submit(Add.builder()
      .withValue(value)
      .withTtl(ttl)
      .build());
  }

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live.
   * @param unit The time to live unit.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, long ttl, TimeUnit unit) {
    return submit(Add.builder()
      .withValue(value)
      .withTtl(ttl, unit)
      .build());
  }

  /**
   * Removes a value from the set.
   *
   * @param value The value to remove.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> remove(T value) {
    return submit(Remove.builder()
      .withValue(value)
      .build());
  }

  /**
   * Checks whether the set contains a value.
   *
   * @param value The value to check.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> contains(Object value) {
    return submit(Contains.builder()
      .withValue(value)
      .build());
  }

  /**
   * Removes all values from the set.
   *
   * @return A completable future to be completed once the operation is complete.
   */
  public CompletableFuture<Void> clear() {
    return submit(Clear.builder().build());
  }

  /**
   * Abstract set command.
   */
  public static abstract class SetCommand<V> implements Command<V>, Writable {

    /**
     * Base set command builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends SetCommand<?>> extends Command.Builder<T, U> {
      protected Builder(U command) {
        super(command);
      }
    }
  }

  /**
   * Abstract set query.
   */
  public static abstract class SetQuery<V> implements Query<V>, Writable {

    /**
     * Base set query builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends SetQuery<?>> extends Query.Builder<T, U> {
      protected Builder(U query) {
        super(query);
      }
    }
  }

  /**
   * Abstract value command.
   */
  public static abstract class ValueCommand<V> extends SetCommand<V> {
    protected Object value;

    /**
     * Returns the value.
     */
    public Object value() {
      return value;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      serializer.writeObject(value, buffer);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      value = serializer.readObject(buffer);
    }

    /**
     * Base key command builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends ValueCommand<?>> extends SetCommand.Builder<T, U> {
      protected Builder(U command) {
        super(command);
      }

      /**
       * Sets the command value.
       *
       * @param value The command value
       * @return The command builder.
       */
      @SuppressWarnings("unchecked")
      public T withValue(Object value) {
        command.value = value;
        return (T) this;
      }
    }
  }

  /**
   * Abstract value query.
   */
  public static abstract class ValueQuery<V> extends SetQuery<V> {
    protected Object value;

    /**
     * Returns the value.
     */
    public Object value() {
      return value;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      serializer.writeObject(value, buffer);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      value = serializer.readObject(buffer);
    }

    /**
     * Base value query builder.
     */
    public static abstract class Builder<T extends Builder<T, U>, U extends ValueQuery<?>> extends SetQuery.Builder<T, U> {
      protected Builder(U query) {
        super(query);
      }

      /**
       * Sets the query value.
       *
       * @param value The query value
       * @return The query builder.
       */
      @SuppressWarnings("unchecked")
      public T withValue(Object value) {
        query.value = value;
        return (T) this;
      }
    }
  }

  /**
   * Contains value command.
   */
  public static class Contains extends ValueQuery<Boolean> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    @Override
    public Consistency consistency() {
      return Consistency.LINEARIZABLE_LEASE;
    }

    /**
     * Contains key builder.
     */
    public static class Builder extends ValueQuery.Builder<Builder, Contains> {
      public Builder() {
        super(new Contains());
      }
    }
  }

  /**
   * TTL command.
   */
  public static abstract class TtlCommand<V> extends ValueCommand<V> {
    protected long ttl;

    /**
     * Returns the time to live in milliseconds.
     *
     * @return The time to live in milliseconds.
     */
    public long ttl() {
      return ttl;
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeLong(ttl);
    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      ttl = buffer.readLong();
    }

    /**
     * TTL command builder.
     */
    public static class Builder<T extends Builder<T, U>, U extends TtlCommand<?>> extends ValueCommand.Builder<T, U> {
      protected Builder(U command) {
        super(command);
      }

      /**
       * Sets the time to live.
       *
       * @param ttl The time to live in milliseconds..
       * @return The command builder.
       */
      public Builder withTtl(long ttl) {
        command.ttl = ttl;
        return this;
      }

      /**
       * Sets the time to live.
       *
       * @param ttl The time to live.
       * @param unit The time to live unit.
       * @return The command builder.
       */
      public Builder withTtl(long ttl, TimeUnit unit) {
        command.ttl = unit.toMillis(ttl);
        return this;
      }
    }
  }

  /**
   * Add command.
   */
  public static class Add extends TtlCommand<Boolean> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    /**
     * Add command builder.
     */
    public static class Builder extends TtlCommand.Builder<Builder, Add> {
      public Builder() {
        super(new Add());
      }
    }
  }

  /**
   * Remove command.
   */
  public static class Remove extends ValueCommand<Boolean> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    /**
     * Remove command builder.
     */
    public static class Builder extends ValueCommand.Builder<Builder, Remove> {
      public Builder() {
        super(new Remove());
      }
    }
  }

  /**
   * Clear command.
   */
  public static class Clear extends SetCommand<Void> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class);
    }

    @Override
    public void writeObject(Buffer buffer, Serializer serializer) {

    }

    @Override
    public void readObject(Buffer buffer, Serializer serializer) {

    }

    /**
     * Get command builder.
     */
    public static class Builder extends SetCommand.Builder<Builder, Clear> {
      public Builder() {
        super(new Clear());
      }
    }
  }

  /**
   * Map state machine.
   */
  public static class StateMachine extends net.kuujo.copycat.raft.StateMachine {
    private final Map<Integer, Commit<? extends TtlCommand>> map = new HashMap<>();

    /**
     * Handles a contains commit.
     */
    @Apply(Contains.class)
    protected boolean containsKey(Commit<Contains> commit) {
      return map.containsKey(commit.operation().value().hashCode());
    }

    /**
     * Handles an add commit.
     */
    @Apply(Add.class)
    protected boolean put(Commit<Add> commit) {
      int hash = commit.operation().value().hashCode();
      if (!map.containsKey(hash)) {
        map.put(hash, commit);
        return true;
      }
      return false;
    }

    /**
     * Filters an add commit.
     */
    @Filter({Add.class})
    protected boolean filterPut(Commit<Add> commit) {
      Commit<? extends TtlCommand> command = map.get(commit.operation().value().hashCode());
      return command != null && command.index() == commit.index() && (command.operation().ttl() == 0 || command.operation().ttl() > System.currentTimeMillis() - command.timestamp());
    }

    /**
     * Handles a remove commit.
     */
    @Apply(Remove.class)
    protected boolean remove(Commit<Remove> commit) {
      Commit<? extends TtlCommand> command = map.remove(commit.operation().value.hashCode());
      return command != null;
    }

    /**
     * Filters a remove commit.
     */
    @Filter(value={Remove.class, Clear.class}, compaction=Compaction.Type.MAJOR)
    protected boolean filterRemove(Commit<?> commit, Compaction compaction) {
      return commit.index() > compaction.index();
    }

    /**
     * Handles a clear commit.
     */
    @Apply(Clear.class)
    protected void clear(Commit<Clear> commit) {
      map.clear();
    }
  }

}
