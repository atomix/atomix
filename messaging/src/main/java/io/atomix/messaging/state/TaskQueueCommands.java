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
package io.atomix.messaging.state;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.copycat.Command;

/**
 * Distributed task queue commands.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class TaskQueueCommands {

  private TaskQueueCommands() {
  }

  /**
   * Base task queue command.
   */
  public static abstract class TaskQueueCommand<T> implements Command<T>, CatalystSerializable {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {

    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {

    }
  }

  /**
   * Submit command.
   */
  public static class Submit extends TaskQueueCommand<Void> {
    private long id;
    private Object task;
    private boolean ack;

    public Submit() {
    }

    public Submit(long id, Object task, boolean ack) {
      this.id = id;
      this.task = task;
      this.ack = ack;
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
     * Returns the task.
     *
     * @return The task.
     */
    public Object task() {
      return task;
    }

    /**
     * Returns whether to acknowledge completion of the task.
     *
     * @return Whether to acknowledge completion of the task.
     */
    public boolean ack() {
      return ack;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      buffer.writeLong(id).writeBoolean(ack);
      serializer.writeObject(task, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      id = buffer.readLong();
      ack = buffer.readBoolean();
      task = serializer.readObject(buffer);
    }
  }

  /**
   * Ack command.
   */
  public static class Ack extends TaskQueueCommand<Object> {
  }

  /**
   * Subscribe command.
   */
  public static class Subscribe extends TaskQueueCommand<Void> {
    @Override
    public Command.CompactionMode compaction() {
      return Command.CompactionMode.QUORUM;
    }

    @Override
    public Command.ConsistencyLevel consistency() {
      return Command.ConsistencyLevel.LINEARIZABLE;
    }
  }

  /**
   * Unsubscribe command.
   */
  public static class Unsubscribe extends TaskQueueCommand<Void> {
    @Override
    public Command.ConsistencyLevel consistency() {
      return Command.ConsistencyLevel.LINEARIZABLE;
    }

    @Override
    public Command.CompactionMode compaction() {
      return Command.CompactionMode.SEQUENTIAL;
    }
  }

  /**
   * Message bus command type resolver.
   */
  public static class TypeResolver implements SerializableTypeResolver {
    @Override
    public void resolve(SerializerRegistry registry) {
      registry.register(Subscribe.class, -151);
      registry.register(Unsubscribe.class, -152);
      registry.register(Submit.class, -107);
      registry.register(Ack.class, -108);
    }
  }

}
