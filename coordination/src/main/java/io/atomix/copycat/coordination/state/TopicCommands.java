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
package io.atomix.copycat.coordination.state;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalogue.client.Command;
import io.atomix.catalogue.client.Operation;
import io.atomix.catalyst.util.BuilderPool;

/**
 * Topic commands.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TopicCommands {

  private TopicCommands() {
  }

  /**
   * Abstract topic command.
   */
  public static abstract class TopicCommand<V> implements Command<V>, CatalystSerializable {
    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {

    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {

    }

    /**
     * Base map command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends TopicCommand<V>, V> extends Command.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }
    }
  }

  /**
   * Listen command.
   */
  @SerializeWith(id=515)
  public static class Listen extends TopicCommand<Void> {

    /**
     * Returns a new listen command builder.
     *
     * @return A new listen command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Listen command builder.
     */
    public static class Builder extends TopicCommand.Builder<Builder, Listen, Void> {
      public Builder(BuilderPool<Builder, Listen> pool) {
        super(pool);
      }

      @Override
      protected Listen create() {
        return new Listen();
      }
    }
  }

  /**
   * Unlisten command.
   */
  @SerializeWith(id=516)
  public static class Unlisten extends TopicCommand<Void> {

    /**
     * Returns a new unlisten command builder.
     *
     * @return A new unlisten command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Unlisten command builder.
     */
    public static class Builder extends TopicCommand.Builder<Builder, Unlisten, Void> {
      public Builder(BuilderPool<Builder, Unlisten> pool) {
        super(pool);
      }

      @Override
      protected Unlisten create() {
        return new Unlisten();
      }
    }
  }

  /**
   * Publish command.
   */
  @SerializeWith(id=517)
  public static class Publish<T> extends TopicCommand<Void> {

    /**
     * Returns a new publish command builder.
     *
     * @param <T> The message type.
     * @return The publish command builder.
     */
    @SuppressWarnings("unchecked")
    public static <T> Builder<T> builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    private T message;

    /**
     * Returns the publish message.
     *
     * @return The publish message.
     */
    public T message() {
      return message;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      serializer.writeObject(message, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      message = serializer.readObject(buffer);
    }

    /**
     * Publish command builder.
     */
    public static class Builder<T> extends TopicCommand.Builder<Builder<T>, Publish<T>, Void> {

      public Builder(BuilderPool<Builder<T>, Publish<T>> pool) {
        super(pool);
      }

      /**
       * Sets the publish command message.
       *
       * @param message The message.
       * @return The publish command builder.
       */
      public Builder<T> withMessage(T message) {
        command.message = message;
        return this;
      }

      @Override
      protected Publish<T> create() {
        return new Publish<>();
      }
    }
  }

}
