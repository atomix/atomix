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

import io.atomix.catalog.client.Command;
import io.atomix.catalog.client.Operation;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.BuilderPool;

import java.util.Map;
import java.util.Set;

/**
 * Message bus commands.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MessageBusCommands {

  private MessageBusCommands() {
  }

  /**
   * Abstract message bus command.
   */
  public static abstract class MessageBusCommand<V> implements Command<V>, CatalystSerializable {
    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {

    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {

    }

    /**
     * Base message bus command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends MessageBusCommand<V>, V> extends Command.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }
    }
  }

  /**
   * Join command.
   */
  @SerializeWith(id=525)
  public static class Join extends MessageBusCommand<Map<String, Set<Address>>> {

    /**
     * Returns a new join command builder.
     *
     * @return A new join command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Join command builder.
     */
    public static class Builder extends MessageBusCommand.Builder<Builder, Join, Map<String, Set<Address>>> {
      public Builder(BuilderPool<Builder, Join> pool) {
        super(pool);
      }

      @Override
      protected Join create() {
        return new Join();
      }
    }
  }

  /**
   * Leave command.
   */
  @SerializeWith(id=526)
  public static class Leave extends MessageBusCommand<Void> {

    /**
     * Returns a new leave command builder.
     *
     * @return A new leave command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Leave command builder.
     */
    public static class Builder extends MessageBusCommand.Builder<Builder, Leave, Void> {
      public Builder(BuilderPool<Builder, Leave> pool) {
        super(pool);
      }

      @Override
      protected Leave create() {
        return new Leave();
      }
    }
  }

  /**
   * Register command.
   */
  @SerializeWith(id=527)
  public static class Register extends MessageBusCommand<Void> {

    /**
     * Returns a new register command builder.
     *
     * @return The register command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    private String topic;

    /**
     * Returns the register topic.
     *
     * @return The register topic.
     */
    public String topic() {
      return topic;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      buffer.writeString(topic);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      topic = buffer.readString();
    }

    /**
     * Register command builder.
     */
    public static class Builder extends MessageBusCommand.Builder<Builder, Register, Void> {

      public Builder(BuilderPool<Builder, Register> pool) {
        super(pool);
      }

      /**
       * Sets the register command message.
       *
       * @param topic The topic.
       * @return The register command builder.
       */
      public Builder withTopic(String topic) {
        command.topic = topic;
        return this;
      }

      @Override
      protected Register create() {
        return new Register();
      }
    }
  }

  /**
   * Unregister command.
   */
  @SerializeWith(id=528)
  public static class Unregister extends MessageBusCommand<Void> {

    /**
     * Returns a new unregister command builder.
     *
     * @return The unregister command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    private String topic;

    /**
     * Returns the unregister topic.
     *
     * @return The unregister topic.
     */
    public String topic() {
      return topic;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      buffer.writeString(topic);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      topic = buffer.readString();
    }

    /**
     * Unregister command builder.
     */
    public static class Builder extends MessageBusCommand.Builder<Builder, Register, Void> {

      public Builder(BuilderPool<Builder, Register> pool) {
        super(pool);
      }

      /**
       * Sets the unregister command message.
       *
       * @param topic The topic.
       * @return The unregister command builder.
       */
      public Builder withTopic(String topic) {
        command.topic = topic;
        return this;
      }

      @Override
      protected Register create() {
        return new Register();
      }
    }
  }

  /**
   * Consumer info.
   */
  public static class ConsumerInfo implements CatalystSerializable {
    private String topic;
    private Address address;

    public ConsumerInfo() {
    }

    public ConsumerInfo(String topic, Address address) {
      this.topic = topic;
      this.address = address;
    }

    public String topic() {
      return topic;
    }

    public Address address() {
      return address;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      buffer.writeString(topic);
      serializer.writeObject(address, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      topic = buffer.readString();
      address = serializer.readObject(buffer);
    }
  }

}
