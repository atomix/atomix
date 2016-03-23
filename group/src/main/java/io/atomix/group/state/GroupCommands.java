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
package io.atomix.group.state;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.Command;
import io.atomix.copycat.Operation;
import io.atomix.copycat.Query;
import io.atomix.group.GroupMemberInfo;
import io.atomix.group.connection.Message;
import io.atomix.group.tasks.Task;

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
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.LINEARIZABLE;
    }

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
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.LINEARIZABLE;
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

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.BOUNDED_LINEARIZABLE;
    }
  }

  /**
   * Join command.
   */
  public static class Join extends MemberCommand<GroupMemberInfo> {
    private Address address;
    private boolean persist;

    public Join() {
    }

    public Join(String member, Address address, boolean persist) {
      super(member);
      this.address = address;
      this.persist = persist;
    }

    /**
     * Returns the member address.
     *
     * @return The member address.
     */
    public Address address() {
      return address;
    }

    /**
     * Returns whether the member is persistent.
     *
     * @return Whether the member is persistent.
     */
    public boolean persist() {
      return persist;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeBoolean(persist);
      serializer.writeObject(address, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      persist = buffer.readBoolean();
      address = serializer.readObject(buffer);
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
  public static class Listen extends GroupCommand<Set<GroupMemberInfo>> {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
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
   * Set property command.
   */
  public static class SetProperty extends PropertyCommand<Void> {
    private Object value;

    public SetProperty() {
    }

    public SetProperty(String member, String property, Object value) {
      super(member, property);
      this.value = value;
    }

    /**
     * Returns the property value.
     *
     * @return The property value.
     */
    public Object value() {
      return value;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(value, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      value = serializer.readObject(buffer);
    }
  }

  /**
   * Get property command.
   */
  public static class GetProperty extends MemberQuery<Object> {
    private String property;

    public GetProperty() {
    }

    public GetProperty(String member, String property) {
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
   * Remove property command.
   */
  public static class RemoveProperty extends PropertyCommand<Void> {
    public RemoveProperty() {
    }

    public RemoveProperty(String member, String property) {
      super(member, property);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Submit command.
   */
  public static class Submit extends MemberCommand<Void> {
    private long id;
    private Object task;

    public Submit() {
    }

    public Submit(String member, long id, Object task) {
      super(member);
      this.id = id;
      this.task = task;
    }

    /**
     * Returns the task ID.
     *
     * @return The task ID.
     */
    public long id() {
      return id;
    }

    /**
     * Returns the message.
     *
     * @return The message.
     */
    public Object task() {
      return task;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeLong(id);
      serializer.writeObject(task, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      id = buffer.readLong();
      task = serializer.readObject(buffer);
    }
  }

  /**
   * Ack command.
   */
  public static class Ack extends MemberCommand<Object> {
    private long id;
    private boolean succeeded;

    public Ack() {
    }

    public Ack(String member, long id, boolean succeeded) {
      super(member);
      this.id = id;
      this.succeeded = succeeded;
    }

    /**
     * Returns the task ID.
     *
     * @return The task ID.
     */
    public long id() {
      return id;
    }

    /**
     * Returns a boolean value indicating whether the task succeeded.
     *
     * @return Indicates whether the task was successfully processed.
     */
    public boolean succeeded() {
      return succeeded;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeLong(id).writeBoolean(succeeded);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      id = buffer.readLong();
      succeeded = buffer.readBoolean();
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
      registry.register(SetProperty.class, -134);
      registry.register(GetProperty.class, -135);
      registry.register(RemoveProperty.class, -136);
      registry.register(Submit.class, -137);
      registry.register(Message.class, -138);
      registry.register(Task.class, -139);
      registry.register(Ack.class, -140);
      registry.register(GroupMemberInfo.class, -158);
    }
  }

}
