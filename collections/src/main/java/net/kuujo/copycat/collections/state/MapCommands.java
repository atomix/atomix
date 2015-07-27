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
import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.ConsistencyLevel;
import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.raft.Query;

import java.util.concurrent.TimeUnit;

/**
 * Map commands.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MapCommands {

  private MapCommands() {
  }

  /**
   * Abstract map command.
   */
  public static abstract class MapCommand<V> implements Command<V>, AlleycatSerializable {

    /**
     * Base map command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends MapCommand<V>, V> extends Command.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }
    }
  }

  /**
   * Abstract map query.
   */
  public static abstract class MapQuery<V> implements Query<V>, AlleycatSerializable {
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
     * Base map query builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends MapQuery<V>, V> extends Query.Builder<T, U, V> {
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
   * Abstract key-based command.
   */
  public static abstract class KeyCommand<V> extends MapCommand<V> {
    protected Object key;

    /**
     * Returns the key.
     */
    public Object key() {
      return key;
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
      alleycat.writeObject(key, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
      key = alleycat.readObject(buffer);
    }

    /**
     * Base key command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends KeyCommand<V>, V> extends MapCommand.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }

      /**
       * Sets the command key.
       *
       * @param key The command key
       * @return The command builder.
       */
      @SuppressWarnings("unchecked")
      public T withKey(Object key) {
        command.key = key;
        return (T) this;
      }
    }
  }

  /**
   * Abstract key-based query.
   */
  public static abstract class KeyQuery<V> extends MapQuery<V> {
    protected Object key;

    /**
     * Returns the key.
     */
    public Object key() {
      return key;
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
      super.writeObject(buffer, alleycat);
      alleycat.writeObject(key, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
      super.readObject(buffer, alleycat);
      key = alleycat.readObject(buffer);
    }

    /**
     * Base key query builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends KeyQuery<V>, V> extends MapQuery.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }

      /**
       * Sets the query key.
       *
       * @param key The query key
       * @return The query builder.
       */
      @SuppressWarnings("unchecked")
      public T withKey(Object key) {
        query.key = key;
        return (T) this;
      }
    }
  }

  /**
   * Contains key command.
   */
  @SerializeWith(id=440)
  public static class ContainsKey extends KeyQuery<Boolean> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Contains key builder.
     */
    public static class Builder extends KeyQuery.Builder<Builder, ContainsKey, Boolean> {
      public Builder(BuilderPool<Builder, ContainsKey> pool) {
        super(pool);
      }

      @Override
      protected ContainsKey create() {
        return new ContainsKey();
      }
    }
  }

  /**
   * Key/value command.
   */
  public static abstract class KeyValueCommand<V> extends KeyCommand<V> {
    protected Object value;

    /**
     * Returns the command value.
     */
    public Object value() {
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
     * Key/value command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends KeyValueCommand<V>, V> extends KeyCommand.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }

      /**
       * Sets the command value.
       *
       * @param value The command value.
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
   * TTL command.
   */
  public static abstract class TtlCommand<V> extends KeyValueCommand<V> {
    protected PersistenceLevel mode = PersistenceLevel.PERSISTENT;
    protected long ttl;

    /**
     * Returns the persistence mode.
     *
     * @return The persistence mode.
     */
    public PersistenceLevel mode() {
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
    public static abstract class Builder<T extends Builder<T, U, V>, U extends TtlCommand<V>, V> extends KeyValueCommand.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
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
   * Put command.
   */
  @SerializeWith(id=441)
  public static class Put extends TtlCommand<Object> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Put command builder.
     */
    public static class Builder extends TtlCommand.Builder<Builder, Put, Object> {
      public Builder(BuilderPool<Builder, Put> pool) {
        super(pool);
      }

      @Override
      protected Put create() {
        return new Put();
      }
    }
  }

  /**
   * Put if absent command.
   */
  @SerializeWith(id=442)
  public static class PutIfAbsent extends TtlCommand<Object> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Put command builder.
     */
    public static class Builder extends TtlCommand.Builder<Builder, PutIfAbsent, Object> {
      public Builder(BuilderPool<Builder, PutIfAbsent> pool) {
        super(pool);
      }

      @Override
      protected PutIfAbsent create() {
        return new PutIfAbsent();
      }
    }
  }

  /**
   * Get query.
   */
  @SerializeWith(id=443)
  public static class Get extends KeyQuery<Object> {

    /**
     * Returns a builder for this query.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Get query builder.
     */
    public static class Builder extends KeyQuery.Builder<Builder, Get, Object> {
      public Builder(BuilderPool<Builder, Get> pool) {
        super(pool);
      }

      @Override
      protected Get create() {
        return new Get();
      }
    }
  }

  /**
   * Get or default query.
   */
  @SerializeWith(id=444)
  public static class GetOrDefault extends KeyQuery<Object> {

    /**
     * Returns a builder for this query.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    private Object defaultValue;

    /**
     * Returns the default value.
     *
     * @return The default value.
     */
    public Object defaultValue() {
      return defaultValue;
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
      super.readObject(buffer, alleycat);
      defaultValue = alleycat.readObject(buffer);
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
      super.writeObject(buffer, alleycat);
      alleycat.writeObject(defaultValue, buffer);
    }

    /**
     * Get command builder.
     */
    public static class Builder extends KeyQuery.Builder<Builder, GetOrDefault, Object> {
      public Builder(BuilderPool<Builder, GetOrDefault> pool) {
        super(pool);
      }

      @Override
      protected GetOrDefault create() {
        return new GetOrDefault();
      }

      /**
       * Sets the default value.
       *
       * @param defaultValue The default value.
       * @return The query builder.
       */
      public Builder withDefaultValue(Object defaultValue) {
        query.defaultValue = defaultValue;
        return this;
      }
    }
  }

  /**
   * Remove command.
   */
  @SerializeWith(id=445)
  public static class Remove extends KeyValueCommand<Object> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Get command builder.
     */
    public static class Builder extends KeyValueCommand.Builder<Builder, Remove, Object> {
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
   * Is empty query.
   */
  @SerializeWith(id=446)
  public static class IsEmpty extends MapQuery<Boolean> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Is empty command builder.
     */
    public static class Builder extends MapQuery.Builder<Builder, IsEmpty, Boolean> {
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
   * Size query.
   */
  @SerializeWith(id=447)
  public static class Size extends MapQuery<Integer> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Is empty command builder.
     */
    public static class Builder extends MapQuery.Builder<Builder, Size, Integer> {
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
   * Clear command.
   */
  @SerializeWith(id=448)
  public static class Clear extends MapCommand<Void> {

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
    public static class Builder extends MapCommand.Builder<Builder, Clear, Void> {
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
