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

/**
 * Distributed queue commands.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueueCommands {

  private QueueCommands() {
  }

  /**
   * Abstract set command.
   */
  private static abstract class SetCommand<V> implements Command<V>, CatalystSerializable {

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
    }

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
  private static abstract class SetQuery<V> implements Query<V>, CatalystSerializable {

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
    }

    /**
     * Base set query builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends SetQuery<V>, V> extends Query.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
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
    public int groupCode() {
      return value.hashCode();
    }

    @Override
    public boolean groupEquals(Command command) {
      return command instanceof ValueCommand && ((ValueCommand) command).value.equals(value);
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      serializer.writeObject(value, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      value = serializer.readObject(buffer);
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
      public T withValue(Object value) {
        if (value == null)
          throw new NullPointerException("value cannot be null");
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
      public T withValue(Object value) {
        if (value == null)
          throw new NullPointerException("value cannot be null");
        query.value = value;
        return (T) this;
      }
    }
  }

  /**
   * Contains value command.
   */
  @SerializeWith(id=470)
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
   * Add command.
   */
  @SerializeWith(id=471)
  public static class Add extends ValueCommand<Boolean> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Add command builder.
     */
    public static class Builder extends ValueCommand.Builder<Builder, Add, Boolean> {
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
   * Offer
   */
  @SerializeWith(id=472)
  public static class Offer extends ValueCommand<Boolean> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Offer command builder.
     */
    public static class Builder extends ValueCommand.Builder<Builder, Offer, Boolean> {
      public Builder(BuilderPool<Builder, Offer> pool) {
        super(pool);
      }

      @Override
      protected Offer create() {
        return new Offer();
      }
    }
  }

  /**
   * Peek command.
   */
  @SerializeWith(id=473)
  public static class Peek extends ValueCommand<Object> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Peek command builder.
     */
    public static class Builder extends ValueCommand.Builder<Builder, Peek, Object> {
      public Builder(BuilderPool<Builder, Peek> pool) {
        super(pool);
      }

      @Override
      protected Peek create() {
        return new Peek();
      }
    }
  }

  /**
   * Poll command.
   */
  @SerializeWith(id=474)
  public static class Poll extends ValueCommand<Object> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Poll command builder.
     */
    public static class Builder extends ValueCommand.Builder<Builder, Poll, Object> {
      public Builder(BuilderPool<Builder, Poll> pool) {
        super(pool);
      }

      @Override
      protected Poll create() {
        return new Poll();
      }
    }
  }

  /**
   * Element command.
   */
  @SerializeWith(id=475)
  public static class Element extends ValueCommand<Object> {

    /**
     * Returns a builder for this command.
     */
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Element command builder.
     */
    public static class Builder extends ValueCommand.Builder<Builder, Element, Object> {
      public Builder(BuilderPool<Builder, Element> pool) {
        super(pool);
      }

      @Override
      protected Element create() {
        return new Element();
      }
    }
  }

  /**
   * Remove command.
   */
  @SerializeWith(id=476)
  public static class Remove extends ValueCommand<Object> {

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
     * Remove command builder.
     */
    public static class Builder extends ValueCommand.Builder<Builder, Remove, Object> {
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
  @SerializeWith(id=477)
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
  @SerializeWith(id=478)
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
  @SerializeWith(id=479)
  public static class Clear extends SetCommand<Void> {

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
