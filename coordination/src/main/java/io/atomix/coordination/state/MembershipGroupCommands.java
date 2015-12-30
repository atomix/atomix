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
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;

import java.util.Set;

/**
 * Group commands.
 * <p>
 * This class reserves serializable type IDs {@code 120} through {@code 124}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class MembershipGroupCommands {

  private MembershipGroupCommands() {
  }

  /**
   * Abstract group command.
   */
  public static abstract class GroupCommand<V> implements Command<V>, CatalystSerializable {

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
   * Abstract group query.
   */
  public static abstract class GroupQuery<V> implements Query<V>, CatalystSerializable {

    @Override
    public ConsistencyLevel consistency() {
      return ConsistencyLevel.BOUNDED_LINEARIZABLE;
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
  @SerializeWith(id=120)
  public static class Join extends GroupCommand<Set<Long>> {
  }

  /**
   * Leave command.
   */
  @SerializeWith(id=121)
  public static class Leave extends GroupCommand<Void> {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Member command.
   */
  public static abstract class MemberCommand<T> extends GroupCommand<T> {
    private long member;

    protected MemberCommand() {
    }

    protected MemberCommand(long member) {
      this.member = member;
    }

    /**
     * Returns the member ID.
     *
     * @return The member ID.
     */
    public long member() {
      return member;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeLong(member);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      member = buffer.readLong();
    }
  }

  /**
   * Schedule command.
   */
  @SerializeWith(id=122)
  public static class Schedule extends MemberCommand<Void> {
    private long delay;
    private Runnable callback;

    public Schedule() {
    }

    public Schedule(long member, long delay, Runnable callback) {
      super(member);
      this.delay = Assert.argNot(delay, delay <= 0, "delay must be positive");
      this.callback = Assert.notNull(callback, "callback");
    }

    /**
     * Returns the delay after which to execute the callback.
     *
     * @return The delay after which to execute the callback.
     */
    public long delay() {
      return delay;
    }

    /**
     * Returns the callback to execute.
     *
     * @return The callback to execute.
     */
    public Runnable callback() {
      return callback;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(callback, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      delay = buffer.readLong();
      callback = serializer.readObject(buffer);
    }
  }

  /**
   * Execute command.
   */
  @SerializeWith(id=123)
  public static class Execute extends MemberCommand<Void> {
    private Runnable callback;

    public Execute() {
    }

    public Execute(long member, Runnable callback) {
      super(member);
      this.callback = Assert.notNull(callback, "callback");
    }

    /**
     * Returns the execute callback.
     *
     * @return The execute callback.
     */
    public Runnable callback() {
      return callback;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      serializer.writeObject(callback, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      callback = serializer.readObject(buffer);
    }
  }

  /**
   * List command.
   */
  @SerializeWith(id=124)
  public static class List extends GroupCommand<Set<Long>> {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }
  }

  /**
   * Property command.
   */
  public static abstract class PropertyCommand<T> extends GroupCommand<T> {
    private String property;

    protected PropertyCommand() {
    }

    protected PropertyCommand(String property) {
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
  @SerializeWith(id=125)
  public static class SetProperty extends PropertyCommand<Void> {
    private Object value;

    public SetProperty() {
    }

    public SetProperty(String property, Object value) {
      super(property);
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
  @SerializeWith(id=126)
  public static class GetProperty extends GroupQuery<Object> {
    private long member;
    private String property;

    public GetProperty() {
    }

    public GetProperty(long member, String property) {
      this.member = member;
      this.property = property;
    }

    /**
     * Returns the member ID.
     *
     * @return The member ID.
     */
    public long member() {
      return member;
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
      buffer.writeLong(member).writeString(property);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      member = buffer.readLong();
      property = buffer.readString();
    }
  }

  /**
   * Remove property command.
   */
  @SerializeWith(id=127)
  public static class RemoveProperty extends PropertyCommand<Void> {
    public RemoveProperty() {
    }

    public RemoveProperty(String property) {
      super(property);
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Send command.
   */
  @SerializeWith(id=118)
  public static class Send extends MemberCommand<Void> {
    private String topic;
    private Object message;

    public Send() {
    }

    public Send(long member, String topic, Object message) {
      super(member);
      this.topic = Assert.notNull(topic, "topic");
      this.message = message;
    }

    /**
     * Returns the topic.
     *
     * @return The topic.
     */
    public String topic() {
      return topic;
    }

    /**
     * Returns the message.
     *
     * @return The message.
     */
    public Object message() {
      return message;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      super.writeObject(buffer, serializer);
      buffer.writeString(topic);
      serializer.writeObject(message, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      super.readObject(buffer, serializer);
      topic = buffer.readString();
      message = serializer.readObject(buffer);
    }
  }

  /**
   * Membership group message.
   */
  @SerializeWith(id=119)
  public static class Message implements CatalystSerializable {
    private String topic;
    private Object body;

    public Message() {
    }

    public Message(String topic, Object body) {
      this.topic = Assert.notNull(topic, "topic");
      this.body = body;
    }

    /**
     * Returns the message topic.
     *
     * @return The message topic.
     */
    public String topic() {
      return topic;
    }

    /**
     * Returns the message body.
     *
     * @return The message body.
     */
    public Object body() {
      return body;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      buffer.writeString(topic);
      serializer.writeObject(body, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      topic = buffer.readString();
      body = serializer.readObject(buffer);
    }

  }

}
