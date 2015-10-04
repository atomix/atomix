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
import io.atomix.catalyst.util.BuilderPool;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Operation;

import java.util.Set;

/**
 * Group commands.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class MembershipGroupCommands {

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
    public void writeObject(BufferOutput buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
    }

    /**
     * Base map command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends GroupCommand<V>, V> extends Command.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }
    }
  }

  /**
   * Join command.
   */
  @SerializeWith(id=520)
  public static class Join extends GroupCommand<Set<Long>> {

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
    public static class Builder extends GroupCommand.Builder<Builder, Join, Set<Long>> {
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
  @SerializeWith(id=521)
  public static class Leave extends GroupCommand<Void> {

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
    public static class Builder extends GroupCommand.Builder<Builder, Leave, Void> {
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
   * Schedule command.
   */
  @SerializeWith(id=522)
  public static class Schedule extends GroupCommand<Void> {

    /**
     * Returns a new schedule command builder.
     *
     * @return The schedule command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    private long member;
    private long delay;
    private Runnable callback;

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

    /**
     * Schedule command builder.
     */
    public static class Builder extends GroupCommand.Builder<Builder, Schedule, Void> {

      public Builder(BuilderPool<Builder, Schedule> pool) {
        super(pool);
      }

      /**
       * Sets the member on which to execute the callback.
       *
       * @param member The member on which to execute the callback.
       * @return The command builder.
       */
      public Builder withMember(long member) {
        command.member = member;
        return this;
      }

      /**
       * Sets the delay after which to execute the callback.
       *
       * @param delay The delay after which to execute the callback.
       * @return The command builder.
       */
      public Builder withDelay(long delay) {
        command.delay = delay;
        return this;
      }

      /**
       * Sets the schedule command message.
       *
       * @param callback The callback.
       * @return The schedule command builder.
       */
      public Builder withCallback(Runnable callback) {
        command.callback = Assert.notNull(callback, "callback");
        return this;
      }

      @Override
      protected Schedule create() {
        return new Schedule();
      }
    }
  }

  /**
   * Execute command.
   */
  @SerializeWith(id=523)
  public static class Execute extends GroupCommand<Void> {

    /**
     * Returns a new execute command builder.
     *
     * @return The execute command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    private long member;
    private Runnable callback;

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

    /**
     * Execute command builder.
     */
    public static class Builder extends GroupCommand.Builder<Builder, Execute, Void> {

      public Builder(BuilderPool<Builder, Execute> pool) {
        super(pool);
      }

      /**
       * Sets the member on which to execute the callback.
       *
       * @param member The member on which to execute the callback.
       * @return The command builder.
       */
      public Builder withMember(long member) {
        command.member = member;
        return this;
      }

      /**
       * Sets the execute command message.
       *
       * @param callback The callback.
       * @return The execute command builder.
       */
      public Builder withCallback(Runnable callback) {
        command.callback = Assert.notNull(callback, "callback");
        return this;
      }

      @Override
      protected Execute create() {
        return new Execute();
      }
    }
  }

}
