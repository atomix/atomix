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
package io.atomix.coordination.state;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.Command;

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
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.LINEARIZABLE;
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
  @SerializeWith(id=525)
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
  @SerializeWith(id=526)
  public static class Leave extends MessageBusCommand<Void> {

    @Override
    public PersistenceLevel persistence() {
      return PersistenceLevel.PERSISTENT;
    }
  }

  /**
   * Register command.
   */
  @SerializeWith(id=527)
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
  @SerializeWith(id=528)
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
    public PersistenceLevel persistence() {
      return PersistenceLevel.PERSISTENT;
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

}
