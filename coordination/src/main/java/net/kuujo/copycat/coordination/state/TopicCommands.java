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
package net.kuujo.copycat.coordination.state;

import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.Operation;
import net.kuujo.copycat.io.BufferInput;
import net.kuujo.copycat.io.BufferOutput;
import net.kuujo.copycat.io.serializer.CopycatSerializable;
import net.kuujo.copycat.io.serializer.Serializer;

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
  public static abstract class TopicCommand<V> implements Command<V>, CopycatSerializable {

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
   * Publish command.
   */
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

  /**
   * Subscribe command.
   */
  public static class Subscribe extends TopicCommand<Void> {

    /**
     * Returns a new publish command builder.
     *
     * @return The publish command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {

    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {

    }

    /**
     * Publish command builder.
     */
    public static class Builder extends TopicCommand.Builder<Builder, Subscribe, Void> {
      public Builder(BuilderPool<Builder, Subscribe> pool) {
        super(pool);
      }

      @Override
      protected Subscribe create() {
        return new Subscribe();
      }
    }
  }

}
