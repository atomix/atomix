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
package io.atomix.resource.internal;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.Command;

/**
 * Wrapper for resource commands.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class ResourceCommand<T extends Command<U>, U> extends ResourceOperation<T, U> implements Command<U> {

  public ResourceCommand() {
  }

  public ResourceCommand(T command) {
    super(command);
  }

  @Override
  public CompactionMode compaction() {
    return operation.compaction();
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
  }

  @Override
  public String toString() {
    return String.format("%s[command=%s]", getClass().getSimpleName(), operation);
  }

  /**
   * Resource register command.
   */
  public static class Register implements Command<Void>, CatalystSerializable {
    private int eventId;

    public Register() {
    }

    public Register(int eventId) {
      this.eventId = eventId;
    }

    /**
     * Returns the event ID.
     *
     * @return The event ID.
     */
    public int event() {
      return eventId;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      buffer.writeByte(eventId);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      eventId = buffer.readByte();
    }
  }

  /**
   * Resource unregister command.
   */
  public static class Unregister implements Command<Void>, CatalystSerializable {
    private int eventId;

    public Unregister() {
    }

    public Unregister(int eventId) {
      this.eventId = eventId;
    }

    /**
     * Returns the event ID.
     *
     * @return The event ID.
     */
    public int event() {
      return eventId;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      buffer.writeByte(eventId);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      eventId = buffer.readByte();
    }
  }

  /**
   * Resource delete command.
   */
  public static class Delete implements Command<Void>, CatalystSerializable {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    }
    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
    }
  }

}
