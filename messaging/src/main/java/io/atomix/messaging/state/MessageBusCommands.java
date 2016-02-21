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
package io.atomix.messaging.state;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Command;
import io.atomix.messaging.Message;

import java.util.Map;
import java.util.Set;

/**
 * Message bus commands.
 * <p>
 * This class reserves serializable type IDs {@code 85} through {@code 89}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class MessageBusCommands {

  private MessageBusCommands() {
  }

  /**
   * Abstract message bus command.
   */
  public static abstract class MessageBusCommand<V> implements Command<V>, CatalystSerializable {

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.LINEARIZABLE;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
    }
  }

  /**
   * Join command.
   */
  public static class Join extends MessageBusCommand<Map<String, Set<Address>>> {
    protected Address address;

    public Join() {
    }

    public Join(Address address) {
      this.address = Assert.notNull(address, "address");
    }

    /**
     * Returns the join member.
     *
     * @return The join member.
     */
    public Address member() {
      return address;
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      address = serializer.readObject(buffer);
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      serializer.writeObject(address, buffer);
    }
  }

  /**
   * Leave command.
   */
  public static class Leave extends MessageBusCommand<Void> {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Register command.
   */
  public static class Register extends MessageBusCommand<Void> {
    private String topic;

    public Register() {
    }

    public Register(String topic) {
      this.topic = Assert.notNull(topic, "topic");
    }

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
  }

  /**
   * Unregister command.
   */
  public static class Unregister extends MessageBusCommand<Void> {
    private String topic;

    public Unregister() {
    }

    public Unregister(String topic) {
      this.topic = Assert.notNull(topic, "topic");
    }

    /**
     * Returns the unregister topic.
     *
     * @return The unregister topic.
     */
    public String topic() {
      return topic;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      buffer.writeString(topic);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      topic = buffer.readString();
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

    /**
     * Returns the consumer topic.
     *
     * @return The consumer topic.
     */
    public String topic() {
      return topic;
    }

    /**
     * Returns the consumer address.
     *
     * @return The consumer address.
     */
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

  /**
   * Message bus command type resolver.
   */
  public static class TypeResolver implements SerializableTypeResolver {
    @Override
    public void resolve(SerializerRegistry registry) {
      registry.register(Join.class, -145);
      registry.register(Leave.class, -146);
      registry.register(Register.class, -147);
      registry.register(Unregister.class, -148);
      registry.register(ConsumerInfo.class, -149);
      registry.register(Message.class, -106);
    }
  }

}
