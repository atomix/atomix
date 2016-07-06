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
package io.atomix.group.internal;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.copycat.Command;
import io.atomix.copycat.Operation;
import io.atomix.copycat.Query;
import io.atomix.group.messaging.MessageProducer;
import io.atomix.group.messaging.internal.GroupMessage;

import java.util.Set;

/**
 * Group commands.
 * <p>
 * This class reserves serializable type IDs {@code 130} through {@code 140} and {@code 158} through {@code 160}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class GroupCommands {

  private GroupCommands() {
  }

  /**
   * Group operation.
   */
  public static abstract class GroupOperation<V> implements Operation<V>, CatalystSerializable {
    @Override
    public void writeObject(BufferOutput<?> bufferOutput, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput<?> bufferInput, Serializer serializer) {
    }
  }

  /**
   * Member operation.
   */
  public static abstract class MemberOperation<T> extends GroupOperation<T> {
    private String member;

    protected MemberOperation() {
    }

    protected MemberOperation(String member) {
      this.member = member;
    }

    /**
     * Returns the member ID.
     *
     * @return The member ID.
     */
    public String member() {
      return member;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeString(member);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      member = buffer.readString();
    }
  }

  /**
   * Abstract group command.
   */
  public static abstract class GroupCommand<V> extends GroupOperation<V> implements Command<V> {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }
  }

  /**
   * Member command.
   */
  public static abstract class MemberCommand<V> extends MemberOperation<V> implements Command<V> {
    public MemberCommand() {
    }

    public MemberCommand(String member) {
      super(member);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }
  }

  /**
   * Group member query.
   */
  public static abstract class MemberQuery<V> extends MemberOperation<V> implements Query<V> {
    public MemberQuery(String member) {
      super(member);
    }

    public MemberQuery() {
    }
  }

  /**
   * Join command.
   */
  public static class Join extends MemberCommand<GroupMemberInfo> {
    private boolean persist;
    private Object metadata;

    public Join() {
    }

    public Join(String member, boolean persist, Object metadata) {
      super(member);
      this.persist = persist;
      this.metadata = metadata;
    }

    /**
     * Returns whether the member is persistent.
     *
     * @return Whether the member is persistent.
     */
    public boolean persist() {
      return persist;
    }

    /**
     * Returns the member metadata.
     *
     * @return The member metadata.
     */
    public Object metadata() {
      return metadata;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeBoolean(persist);
      serializer.writeObject(metadata, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      persist = buffer.readBoolean();
      metadata = serializer.readObject(buffer);
    }
  }

  /**
   * Leave command.
   */
  public static class Leave extends MemberCommand<Void> {
    public Leave() {
    }

    public Leave(String member) {
      super(member);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * List command.
   */
  public static class Listen extends MemberCommand<GroupStatus> {
    public Listen() {
    }

    public Listen(String member) {
      super(member);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }
  }

  /**
   * Group status.
   */
  public static class GroupStatus implements CatalystSerializable {
    private long term;
    private String leader;
    private Set<GroupMemberInfo> members;

    public GroupStatus() {
    }

    public GroupStatus(long term, String leader, Set<GroupMemberInfo> members) {
      this.term = term;
      this.leader = leader;
      this.members = members;
    }

    public long term() {
      return term;
    }

    public String leader() {
      return leader;
    }

    public Set<GroupMemberInfo> members() {
      return members;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      buffer.writeLong(term).writeString(leader);
      serializer.writeObject(members, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      term = buffer.readLong();
      leader = buffer.readString();
      members = serializer.readObject(buffer);
    }
  }

  /**
   * Property command.
   */
  public static abstract class PropertyCommand<T> extends MemberCommand<T> {
    private String property;

    protected PropertyCommand() {
    }

    protected PropertyCommand(String member, String property) {
      super(member);
      this.property = property;
    }

    /**
     * Returns the property name.
     *
     * @return The property name.
     */
    public String property() {
      return property;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeString(property);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      property = buffer.readString();
    }
  }

  /**
   * Message command.
   */
  public static class Message extends MemberCommand<Void> {
    private int producer;
    private long id;
    private String queue;
    private Object message;
    private MessageProducer.Delivery delivery;
    private MessageProducer.Execution execution;

    public Message() {
    }

    public Message(String member, int producer, String queue, long id, Object message, MessageProducer.Delivery delivery, MessageProducer.Execution execution) {
      super(member);
      this.producer = producer;
      this.queue = queue;
      this.id = id;
      this.message = message;
      this.delivery = delivery;
      this.execution = execution;
    }

    /**
     * Returns the producer ID.
     *
     * @return The producer ID.
     */
    public int producer() {
      return producer;
    }

    /**
     * Returns the message queue name.
     *
     * @return The message queue name.
     */
    public String queue() {
      return queue;
    }

    /**
     * Returns the message ID.
     *
     * @return The message ID.
     */
    public long id() {
      return id;
    }

    /**
     * Returns the message.
     *
     * @return The message.
     */
    public Object message() {
      return message;
    }

    /**
     * Returns the message delivery policy.
     *
     * @return The message delivery policy.
     */
    public MessageProducer.Delivery delivery() {
      return delivery;
    }

    /**
     * Returns the message execution policy.
     *
     * @return The message execution policy.
     */
    public MessageProducer.Execution execution() {
      return execution;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeUnsignedShort(producer);
      buffer.writeString(queue);
      buffer.writeLong(id);
      buffer.writeByte(delivery.ordinal());
      buffer.writeByte(execution.ordinal());
      serializer.writeObject(message, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      producer = buffer.readUnsignedShort();
      queue = buffer.readString();
      id = buffer.readLong();
      delivery = MessageProducer.Delivery.values()[buffer.readByte()];
      execution = MessageProducer.Execution.values()[buffer.readByte()];
      message = serializer.readObject(buffer);
    }
  }

  /**
   * Reply command.
   */
  public static class Reply extends MemberCommand<Void> {
    private String queue;
    private long id;
    private boolean succeeded;
    private Object message;

    public Reply() {
    }

    public Reply(String member, String queue, long id, boolean succeeded, Object message) {
      super(member);
      this.queue = queue;
      this.id = id;
      this.succeeded = succeeded;
      this.message = message;
    }

    /**
     * Returns the queue name.
     *
     * @return The queue name.
     */
    public String queue() {
      return queue;
    }

    /**
     * Returns the message ID.
     *
     * @return The message ID.
     */
    public long id() {
      return id;
    }

    /**
     * Returns whether the reply succeeded.
     *
     * @return Whether the reply succeeded.
     */
    public boolean succeeded() {
      return succeeded;
    }

    /**
     * Returns the reply message.
     *
     * @return The reply message.
     */
    public Object message() {
      return message;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeString(queue);
      buffer.writeLong(id);
      buffer.writeBoolean(succeeded);
      serializer.writeObject(message, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      queue = buffer.readString();
      id = buffer.readLong();
      succeeded = buffer.readBoolean();
      message = serializer.readObject(buffer);
    }
  }

  /**
   * Ack command.
   */
  public static class Ack extends MemberCommand<Void> {
    private int producer;
    private String queue;
    private long id;
    private boolean succeeded;
    private Object message;

    public Ack() {
    }

    public Ack(String member, int producer, String queue, long id, boolean succeeded, Object message) {
      super(member);
      this.producer = producer;
      this.queue = queue;
      this.id = id;
      this.succeeded = succeeded;
      this.message = message;
    }

    /**
     * Returns the producer ID.
     *
     * @return The producer ID.
     */
    public int producer() {
      return producer;
    }

    /**
     * Returns the queue name.
     *
     * @return The queue name.
     */
    public String queue() {
      return queue;
    }

    /**
     * Returns the message ID.
     *
     * @return The message ID.
     */
    public long id() {
      return id;
    }

    /**
     * Returns whether the message succeeded.
     *
     * @return Whether the message succeeded.
     */
    public boolean succeeded() {
      return succeeded;
    }

    /**
     * Returns the reply message.
     *
     * @return The reply message.
     */
    public Object message() {
      return message;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeUnsignedShort(producer);
      buffer.writeString(queue);
      buffer.writeLong(id);
      buffer.writeBoolean(succeeded);
      serializer.writeObject(message, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      producer = buffer.readUnsignedShort();
      queue = buffer.readString();
      id = buffer.readLong();
      succeeded = buffer.readBoolean();
      message = serializer.readObject(buffer);
    }
  }

  /**
   * Group command type resolver.
   */
  public static class TypeResolver implements SerializableTypeResolver {
    @Override
    public void resolve(SerializerRegistry registry) {
      registry.register(Join.class, -130);
      registry.register(Leave.class, -131);
      registry.register(Listen.class, -132);
      registry.register(Message.class, -137);
      registry.register(Reply.class, -138);
      registry.register(Ack.class, -139);
      registry.register(GroupMessage.class, -140);
      registry.register(GroupMemberInfo.class, -158);
      registry.register(GroupStatus.class, -159);
    }
  }

}
