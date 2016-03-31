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
package io.atomix.variables.internal;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.copycat.Command;

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
  public static class IncrementAndGet extends LongCommand<Long> {
  }

  /**
   * Decrement and get command.
   */
  public static class DecrementAndGet extends LongCommand<Long> {
  }

  /**
   * Get and increment command.
   */
  public static class GetAndIncrement extends LongCommand<Long> {
  }

  /**
   * Get and decrement command.
   */
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
  public static class AddAndGet extends DeltaCommand {
    public AddAndGet() {
    }

    public AddAndGet(long delta) {
      super(delta);
    }
  }

  /**
   * Value command type resolver.
   */
  public static class TypeResolver implements SerializableTypeResolver {
    @Override
    public void resolve(SerializerRegistry registry) {
      registry.register(ValueCommands.CompareAndSet.class, -110);
      registry.register(ValueCommands.Get.class, -111);
      registry.register(ValueCommands.GetAndSet.class, -112);
      registry.register(ValueCommands.Set.class, -113);
      registry.register(IncrementAndGet.class, -114);
      registry.register(DecrementAndGet.class, -115);
      registry.register(GetAndIncrement.class, -116);
      registry.register(GetAndDecrement.class, -117);
      registry.register(AddAndGet.class, -118);
      registry.register(GetAndAdd.class, -119);
    }
  }

}
