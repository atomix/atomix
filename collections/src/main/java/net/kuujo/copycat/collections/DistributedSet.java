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

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.AlleycatSerializable;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.Mode;
import net.kuujo.copycat.Resource;
import net.kuujo.copycat.Stateful;
import net.kuujo.copycat.log.Compaction;
import net.kuujo.copycat.raft.*;
import net.kuujo.copycat.raft.server.Apply;
import net.kuujo.copycat.raft.server.Commit;
import net.kuujo.copycat.raft.server.Filter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous set.
 *
 * @param <T> The set value type.
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Stateful(DistributedSet.StateMachine.class)
public class DistributedSet<T> extends Resource {

  public DistributedSet(Raft protocol) {
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
      .withValue(value.hashCode())
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
      .withValue(value.hashCode())
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
      .withValue(value.hashCode())
      .withTtl(ttl, unit)
      .build());
  }

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live in milliseconds.
   * @param mode The persistence mode.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, long ttl, Mode mode) {
    return submit(Add.builder()
      .withValue(value.hashCode())
      .withTtl(ttl)
      .withMode(mode)
      .build());
  }

  /**
   * Adds a value to the set with a TTL.
   *
   * @param value The value to add.
   * @param ttl The time to live.
   * @param unit The time to live unit.
   * @param mode The persistence mode.
   * @return A completable future to be completed with the result once complete.
   */
  @SuppressWarnings("unchecked")
  public CompletableFuture<Boolean> add(T value, long ttl, TimeUnit unit, Mode mode) {
    return submit(Add.builder()
      .withValue(value.hashCode())
      .withTtl(ttl, unit)
      .withMode(mode)
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
      .withValue(value.hashCode())
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
      .withValue(value.hashCode())
      .build());
  }

  /**
   * Checks whether the set contains a value.
   *
   * @param value The value to check.
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the result once complete.
   */
  public CompletableFuture<Boolean> contains(Object value, ConsistencyLevel consistency) {
    return submit(Contains.builder()
      .withValue(value.hashCode())
      .withConsistency(consistency)
      .build());
  }

  /**
   * Gets the set size.
   *
   * @return A completable future to be completed with the set size.
   */
  public CompletableFuture<Integer> size() {
    return submit(Size.builder().build());
  }

  /**
   * Gets the set size.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with the set size.
   */
  public CompletableFuture<Integer> size(ConsistencyLevel consistency) {
    return submit(Size.builder().withConsistency(consistency).build());
  }

  /**
   * Checks whether the set is empty.
   *
   * @return A completable future to be completed with a boolean value indicating whether the set is empty.
   */
  public CompletableFuture<Boolean> isEmpty() {
    return submit(IsEmpty.builder().build());
  }

  /**
   * Checks whether the set is empty.
   *
   * @param consistency The query consistency level.
   * @return A completable future to be completed with a boolean value indicating whether the set is empty.
   */
  public CompletableFuture<Boolean> isEmpty(ConsistencyLevel consistency) {
    return submit(IsEmpty.builder().withConsistency(consistency).build());
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
  public static abstract class SetCommand<V> implements Command<V>, AlleycatSerializable {

    /**
     * Base set command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends SetCommand<V>, V> extends Command.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }
    }
  }

  /**
   * Abstract set query.
   */
  public static abstract class SetQuery<V> implements Query<V>, AlleycatSerializable {
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
     * Base set query builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends SetQuery<V>, V> extends Query.Builder<T, U, V> {
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
        query.consistency = consistency;
        return (T) this;
      }
    }
  }

  /**
   * Abstract value command.
   */
  public static abstract class ValueCommand<V> extends SetCommand<V> {
    protected int value;

    /**
     * Returns the value.
     */
    public int value() {
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

    /**
     * Base key command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends ValueCommand<V>, V> extends SetCommand.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }

      /**
       * Sets the command value.
       *
       * @param value The command value
       * @return The command builder.
       */
      @SuppressWarnings("unchecked")
      public T withValue(int value) {
        command.value = value;
        return (T) this;
      }
    }
  }

  /**
   * Abstract value query.
   */
  public static abstract class ValueQuery<V> extends SetQuery<V> {
    protected int value;

    /**
     * Returns the value.
     */
    public int value() {
      return value;
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
      super.writeObject(buffer, alleycat);
      alleycat.writeObject(value, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
      super.readObject(buffer, alleycat);
      value = alleycat.readObject(buffer);
    }

    /**
     * Base value query builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends ValueQuery<V>, V> extends SetQuery.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }

      /**
       * Sets the query value.
       *
       * @param value The query value
       * @return The query builder.
       */
      @SuppressWarnings("unchecked")
      public T withValue(int value) {
        query.value = value;
        return (T) this;
      }
    }
  }

  /**
   * Contains value command.
   */
  @SerializeWith(id=450)
  public static class Contains extends ValueQuery<Boolean> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Contains key builder.
     */
    public static class Builder extends ValueQuery.Builder<Builder, Contains, Boolean> {
      public Builder(BuilderPool<Builder, Contains> pool) {
        super(pool);
      }

      @Override
      protected Contains create() {
        return new Contains();
      }
    }
  }

  /**
   * TTL command.
   */
  public static abstract class TtlCommand<V> extends ValueCommand<V> {
    protected long ttl;
    protected Mode mode = Mode.PERSISTENT;

    /**
     * Returns the time to live in milliseconds.
     *
     * @return The time to live in milliseconds.
     */
    public long ttl() {
      return ttl;
    }

    /**
     * Returns the persistence mode.
     *
     * @return The persistence mode.
     */
    public Mode mode() {
      return mode;
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
      super.writeObject(buffer, alleycat);
      buffer.writeByte(mode.ordinal()).writeLong(ttl);
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
      super.readObject(buffer, alleycat);
      mode = Mode.values()[buffer.readByte()];
      ttl = buffer.readLong();
    }

    /**
     * TTL command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends TtlCommand<V>, V> extends ValueCommand.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
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

      /**
       * Sets the persistence mode.
       *
       * @param mode The persistence mode.
       * @return The command builder.
       */
      public Builder withMode(Mode mode) {
        command.mode = mode;
        return this;
      }
    }
  }

  /**
   * Add command.
   */
  @SerializeWith(id=451)
  public static class Add extends TtlCommand<Boolean> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Add command builder.
     */
    public static class Builder extends TtlCommand.Builder<Builder, Add, Boolean> {
      public Builder(BuilderPool<Builder, Add> pool) {
        super(pool);
      }

      @Override
      protected Add create() {
        return new Add();
      }
    }
  }

  /**
   * Remove command.
   */
  @SerializeWith(id=452)
  public static class Remove extends ValueCommand<Boolean> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Remove command builder.
     */
    public static class Builder extends ValueCommand.Builder<Builder, Remove, Boolean> {
      public Builder(BuilderPool<Builder, Remove> pool) {
        super(pool);
      }

      @Override
      protected Remove create() {
        return new Remove();
      }
    }
  }

  /**
   * Size query.
   */
  @SerializeWith(id=453)
  public static class Size extends SetQuery<Integer> {

    /**
     * Returns a builder for this query.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Size query builder.
     */
    public static class Builder extends SetQuery.Builder<Builder, Size, Integer> {
      public Builder(BuilderPool<Builder, Size> pool) {
        super(pool);
      }

      @Override
      protected Size create() {
        return new Size();
      }
    }
  }

  /**
   * Is empty query.
   */
  @SerializeWith(id=454)
  public static class IsEmpty extends SetQuery<Boolean> {

    /**
     * Returns a builder for this query.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Is empty query builder.
     */
    public static class Builder extends SetQuery.Builder<Builder, IsEmpty, Boolean> {
      public Builder(BuilderPool<Builder, IsEmpty> pool) {
        super(pool);
      }

      @Override
      protected IsEmpty create() {
        return new IsEmpty();
      }
    }
  }

  /**
   * Clear command.
   */
  @SerializeWith(id=455)
  public static class Clear extends SetCommand<Void> {

    /**
     * Returns a builder for this command.
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
     * Get command builder.
     */
    public static class Builder extends SetCommand.Builder<Builder, Clear, Void> {
      public Builder(BuilderPool<Builder, Clear> pool) {
        super(pool);
      }

      @Override
      protected Clear create() {
        return new Clear();
      }
    }
  }

  /**
   * Map state machine.
   */
  public static class StateMachine extends net.kuujo.copycat.raft.server.StateMachine {
    private final Map<Integer, Commit<? extends TtlCommand>> map = new HashMap<>();
    private final Set<Long> sessions = new HashSet<>();
    private long time;

    /**
     * Updates the wall clock time.
     */
    private void updateTime(Commit<?> commit) {
      time = Math.max(time, commit.timestamp());
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
    private boolean isActive(Commit<? extends TtlCommand> commit) {
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
     * Handles a contains commit.
     */
    @Apply(Contains.class)
    protected boolean contains(Commit<Contains> commit) {
      updateTime(commit);
      Commit<? extends TtlCommand> command = map.get(commit.operation().value());
      if (!isActive(command)) {
        map.remove(commit.operation().value());
        return false;
      }
      return true;
    }

    /**
     * Handles an add commit.
     */
    @Apply(Add.class)
    protected boolean put(Commit<Add> commit) {
      updateTime(commit);
      Commit<? extends TtlCommand> command = map.get(commit.operation().value());
      if (!isActive(command)) {
        map.put(commit.operation().value(), commit);
        return true;
      }
      return false;
    }

    /**
     * Filters an add commit.
     */
    @Filter({Add.class})
    protected boolean filterPut(Commit<Add> commit) {
      Commit<? extends TtlCommand> command = map.get(commit.operation().value());
      return command != null && command.index() == commit.index() && isActive(command);
    }

    /**
     * Handles a remove commit.
     */
    @Apply(Remove.class)
    protected boolean remove(Commit<Remove> commit) {
      updateTime(commit);
      Commit<? extends TtlCommand> command = map.remove(commit.operation().value());
      return isActive(command);
    }

    /**
     * Filters a remove commit.
     */
    @Filter(value={Remove.class, Clear.class}, compaction=Compaction.Type.MAJOR)
    protected boolean filterRemove(Commit<?> commit, Compaction compaction) {
      return commit.index() > compaction.index();
    }

    /**
     * Handles a size commit.
     */
    @Apply(Size.class)
    protected int size(Commit<Size> commit) {
      updateTime(commit);
      return map.size();
    }

    /**
     * Handles an is empty commit.
     */
    @Apply(IsEmpty.class)
    protected boolean isEmpty(Commit<IsEmpty> commit) {
      updateTime(commit);
      return map.isEmpty();
    }

    /**
     * Handles a clear commit.
     */
    @Apply(Clear.class)
    protected void clear(Commit<Clear> commit) {
      updateTime(commit);
      map.clear();
    }
  }

}
