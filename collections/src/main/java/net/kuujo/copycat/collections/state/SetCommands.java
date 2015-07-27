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
package net.kuujo.copycat.collections.state;

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.AlleycatSerializable;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.PersistenceLevel;
import net.kuujo.copycat.Command;
import net.kuujo.copycat.ConsistencyLevel;
import net.kuujo.copycat.Operation;
import net.kuujo.copycat.Query;

import java.util.concurrent.TimeUnit;

/**
 * Distributed set commands.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SetCommands {

  private SetCommands() {
  }

  /**
   * Abstract set command.
   */
  private static abstract class SetCommand<V> implements Command<V>, AlleycatSerializable {

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
  private static abstract class SetQuery<V> implements Query<V>, AlleycatSerializable {
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
  private static abstract class ValueCommand<V> extends SetCommand<V> {
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
  private static abstract class ValueQuery<V> extends SetQuery<V> {
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
    protected PersistenceLevel mode = PersistenceLevel.PERSISTENT;

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
    public PersistenceLevel mode() {
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
      mode = PersistenceLevel.values()[buffer.readByte()];
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
      public Builder withPersistence(PersistenceLevel mode) {
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

}
