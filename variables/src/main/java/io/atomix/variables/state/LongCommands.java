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
 * limitations under the License
 */
package io.atomix.variables.state;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.client.Command;

/**
 * Long commands.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class LongCommands {

  private LongCommands() {
  }

  /**
   * Abstract long command.
   */
  public static abstract class LongCommand<V> implements Command<V>, CatalystSerializable {

    protected LongCommand() {
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SNAPSHOT;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
    }
  }

  /**
   * Increment and get command.
   */
  @SerializeWith(id=54)
  public static class IncrementAndGet extends LongCommand<Long> {
  }

  /**
   * Decrement and get command.
   */
  @SerializeWith(id=55)
  public static class DecrementAndGet extends LongCommand<Long> {
  }

  /**
   * Get and increment command.
   */
  @SerializeWith(id=56)
  public static class GetAndIncrement extends LongCommand<Long> {
  }

  /**
   * Get and decrement command.
   */
  @SerializeWith(id=57)
  public static class GetAndDecrement extends LongCommand<Long> {
  }

  /**
   * Delta command.
   */
  public static abstract class DeltaCommand extends LongCommand<Long> {
    private long delta;

    public DeltaCommand() {
    }

    public DeltaCommand(long delta) {
      this.delta = delta;
    }

    /**
     * Returns the delta.
     *
     * @return The delta.
     */
    public long delta() {
      return delta;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      buffer.writeLong(delta);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      delta = buffer.readLong();
    }
  }

  /**
   * Get and add command.
   */
  @SerializeWith(id=58)
  public static class GetAndAdd extends DeltaCommand {
    public GetAndAdd() {
    }

    public GetAndAdd(long delta) {
      super(delta);
    }
  }

  /**
   * Add and get command.
   */
  @SerializeWith(id=59)
  public static class AddAndGet extends DeltaCommand {
    public AddAndGet() {
    }

    public AddAndGet(long delta) {
      super(delta);
    }
  }

}
