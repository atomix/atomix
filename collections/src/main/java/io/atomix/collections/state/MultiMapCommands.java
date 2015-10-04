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
package io.atomix.collections.state;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.BuilderPool;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Operation;
import io.atomix.copycat.client.Query;

import java.util.Collection;

/**
 * Map commands.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MultiMapCommands {

  private MultiMapCommands() {
  }

  /**
   * Abstract map command.
   */
  public static abstract class MapCommand<V> implements Command<V>, CatalystSerializable {

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
    }

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
  public static abstract class MapQuery<V> implements Query<V>, CatalystSerializable {

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
    }

    /**
     * Base map query builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends MapQuery<V>, V> extends Query.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
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
    public int groupCode() {
      return key.hashCode();
    }

    @Override
    public boolean groupEquals(Command command) {
      return command instanceof KeyCommand && ((KeyCommand) command).key.equals(key);
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      serializer.writeObject(key, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      key = serializer.readObject(buffer);
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
        if (key == null)
          throw new NullPointerException("key cannot be null");
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
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(key, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      key = serializer.readObject(buffer);
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
        if (key == null)
          throw new NullPointerException("key cannot be null");
        query.key = key;
        return (T) this;
      }
    }
  }

  /**
   * Abstract value-based query.
   */
  public static abstract class ValueQuery<V> extends MapQuery<V> {
    protected Object value;

    /**
     * Returns the value.
     */
    public Object value() {
      return value;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(value, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      value = serializer.readObject(buffer);
    }

    /**
     * Base key query builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends ValueQuery<V>, V> extends MapQuery.Builder<T, U, V> {
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
      public T withValue(Object value) {
        if (value == null)
          throw new NullPointerException("value cannot be null");
        query.value = value;
        return (T) this;
      }
    }
  }

  /**
   * Entry query.
   */
  public static class EntryQuery<V> extends KeyQuery<V> {
    protected Object value;

    /**
     * Returns the value.
     *
     * @return The value.
     */
    public Object value() {
      return value;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(value, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      value = serializer.readObject(buffer);
    }

    /**
     * Entry query builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends EntryQuery<V>, V> extends KeyQuery.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }

      /**
       * Sets the entry value.
       *
       * @param value The entry value.
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
   * Contains key query.
   */
  @SerializeWith(id=460)
  public static class ContainsKey extends KeyQuery<Boolean> {

    /**
     * Returns a builder for this query.
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
   * Contains entry query.
   */
  @SerializeWith(id=461)
  public static class ContainsEntry extends EntryQuery<Boolean> {

    /**
     * Returns a builder for this query.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Contains key builder.
     */
    public static class Builder extends EntryQuery.Builder<Builder, ContainsEntry, Boolean> {
      public Builder(BuilderPool<Builder, ContainsEntry> pool) {
        super(pool);
      }

      @Override
      protected ContainsEntry create() {
        return new ContainsEntry();
      }
    }
  }

  /**
   * Contains value query.
   */
  @SerializeWith(id=462)
  public static class ContainsValue extends ValueQuery<Boolean> {

    /**
     * Returns a builder for this query.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Contains key builder.
     */
    public static class Builder extends ValueQuery.Builder<Builder, ContainsValue, Boolean> {
      public Builder(BuilderPool<Builder, ContainsValue> pool) {
        super(pool);
      }

      @Override
      protected ContainsValue create() {
        return new ContainsValue();
      }
    }
  }

  /**
   * Entry command.
   */
  public static abstract class EntryCommand<V> extends KeyCommand<V> {
    protected Object value;

    /**
     * Returns the command value.
     */
    public Object value() {
      return value;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(value, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      value = serializer.readObject(buffer);
    }

    /**
     * Key/value command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends EntryCommand<V>, V> extends KeyCommand.Builder<T, U, V> {
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
  public static abstract class TtlCommand<V> extends EntryCommand<V> {
    protected long ttl;

    @Override
    public PersistenceLevel persistence() {
      return ttl > 0 ? PersistenceLevel.EPHEMERAL : PersistenceLevel.PERSISTENT;
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
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeLong(ttl);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      ttl = buffer.readLong();
    }

    /**
     * TTL command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends TtlCommand<V>, V> extends EntryCommand.Builder<T, U, V> {
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
    }
  }

  /**
   * Put command.
   */
  @SerializeWith(id=463)
  public static class Put extends TtlCommand<Boolean> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Put command builder.
     */
    public static class Builder extends TtlCommand.Builder<Builder, Put, Boolean> {
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
   * Get query.
   */
  @SerializeWith(id=464)
  public static class Get extends KeyQuery<Collection> {

    /**
     * Returns a builder for this query.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Get query builder.
     */
    public static class Builder extends KeyQuery.Builder<Builder, Get, Collection> {
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
   * Remove command.
   */
  @SerializeWith(id=465)
  public static class Remove extends EntryCommand<Object> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    @Override
    public PersistenceLevel persistence() {
      return PersistenceLevel.EPHEMERAL;
    }

    /**
     * Get command builder.
     */
    public static class Builder extends EntryCommand.Builder<Builder, Remove, Object> {
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
  @SerializeWith(id=466)
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
  @SerializeWith(id=467)
  public static class Size extends KeyQuery<Integer> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Is empty command builder.
     */
    public static class Builder extends KeyQuery.Builder<Builder, Size, Integer> {
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
  @SerializeWith(id=468)
  public static class Clear extends MapCommand<Void> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    @Override
    public PersistenceLevel persistence() {
      return PersistenceLevel.EPHEMERAL;
    }

    @Override
    public boolean groupEquals(Command command) {
      return command instanceof Clear;
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
