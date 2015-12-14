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
   * Abstract topic command.
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
   * Schedule command.
   */
  @SerializeWith(id=122)
  public static class Schedule extends GroupCommand<Void> {
    private long member;
    private long delay;
    private Runnable callback;

    public Schedule() {
    }

    public Schedule(long member, long delay, Runnable callback) {
      this.member = Assert.argNot(member, member <= 0, "member must be positive");
      this.delay = Assert.argNot(delay, delay <= 0, "delay must be positive");
      this.callback = Assert.notNull(callback, "callback");
    }

    /**
     * Returns the member on which to execute the callback.
     *
     * @return The member on which to execute the callback.
     */
    public long member() {
      return member;
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
      buffer.writeLong(member).writeLong(delay);
      serializer.writeObject(callback, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      member = buffer.readLong();
      delay = buffer.readLong();
      callback = serializer.readObject(buffer);
    }
  }

  /**
   * Execute command.
   */
  @SerializeWith(id=123)
  public static class Execute extends GroupCommand<Void> {
    private long member;
    private Runnable callback;

    public Execute() {
    }

    public Execute(long member, Runnable callback) {
      this.member = Assert.argNot(member, member <= 0, "member must be positive");
      this.callback = Assert.notNull(callback, "callback");
    }

    /**
     * Returns the member on which to execute the callback.
     *
     * @return The member on which to execute the callback.
     */
    public long member() {
      return member;
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
      buffer.writeLong(member);
      serializer.writeObject(callback, buffer);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      member = buffer.readLong();
      callback = serializer.readObject(buffer);
    }
  }

}
