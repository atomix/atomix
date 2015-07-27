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

import net.kuujo.alleycat.Alleycat;
import net.kuujo.alleycat.AlleycatSerializable;
import net.kuujo.alleycat.SerializeWith;
import net.kuujo.alleycat.io.BufferInput;
import net.kuujo.alleycat.io.BufferOutput;
import net.kuujo.copycat.BuilderPool;
import net.kuujo.copycat.raft.Command;
import net.kuujo.copycat.raft.Operation;

import java.util.concurrent.TimeUnit;

/**
 * Lock commands.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LockCommands {

  private LockCommands() {
  }

  /**
   * Abstract lock command.
   */
  public static abstract class LockCommand<V> implements Command<V>, AlleycatSerializable {
    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
    }

    /**
     * Base reference command builder.
     */
    public static abstract class Builder<T extends Builder<T, U, V>, U extends LockCommand<V>, V> extends Command.Builder<T, U, V> {
      protected Builder(BuilderPool<T, U> pool) {
        super(pool);
      }
    }
  }

  /**
   * Lock command.
   */
  @SerializeWith(id=512)
  public static class Lock extends LockCommand<Boolean> {

    /**
     * Returns a new lock command builder.
     *
     * @return A new lock command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    private long timeout;

    /**
     * Returns the try lock timeout.
     *
     * @return The try lock timeout in milliseconds.
     */
    public long timeout() {
      return timeout;
    }

    @Override
    public void writeObject(BufferOutput buffer, Alleycat alleycat) {
      buffer.writeLong(timeout);
    }

    @Override
    public void readObject(BufferInput buffer, Alleycat alleycat) {
      timeout = buffer.readLong();
    }

    /**
     * Try lock builder.
     */
    public static class Builder extends LockCommand.Builder<Builder, Lock, Boolean> {
      public Builder(BuilderPool<Builder, Lock> pool) {
        super(pool);
      }

      @Override
      protected void reset(Lock command) {
        super.reset(command);
        command.timeout = 0;
      }

      /**
       * Sets the lock timeout.
       *
       * @param timeout The lock timeout in milliseconds.
       * @return The command builder.
       */
      public Builder withTimeout(long timeout) {
        if (timeout < -1)
          throw new IllegalArgumentException("timeout cannot be less than -1");
        command.timeout = timeout;
        return this;
      }

      /**
       * Sets the lock timeout.
       *
       * @param timeout The lock timeout.
       * @param unit The lock timeout time unit.
       * @return The command builder.
       */
      public Builder withTimeout(long timeout, TimeUnit unit) {
        return withTimeout(unit.toMillis(timeout));
      }

      @Override
      protected Lock create() {
        return new Lock();
      }
    }
  }

  /**
   * Unlock command.
   */
  @SerializeWith(id=513)
  public static class Unlock extends LockCommand<Void> {

    /**
     * Returns a new unlock command builder.
     *
     * @return A new unlock command builder.
     */
    @SuppressWarnings("unchecked")
    public static Builder builder() {
      return Operation.builder(Builder.class, Builder::new);
    }

    /**
     * Unlock command builder.
     */
    public static class Builder extends LockCommand.Builder<Builder, Unlock, Void> {
      public Builder(BuilderPool<Builder, Unlock> pool) {
        super(pool);
      }

      @Override
      protected Unlock create() {
        return new Unlock();
      }
    }
  }

}
