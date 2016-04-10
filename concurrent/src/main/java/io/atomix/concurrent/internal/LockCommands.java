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
package io.atomix.concurrent.internal;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializableTypeResolver;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.SerializerRegistry;
import io.atomix.copycat.Command;

/**
 * Lock commands.
 * <p>
 * This class reserves serializable type IDs {@code 115} through {@code 118}
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class LockCommands {

  private LockCommands() {
  }

  /**
   * Abstract lock command.
   */
  public static abstract class LockCommand<V> implements Command<V>, CatalystSerializable {
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
   * Lock command.
   */
  public static class Lock extends LockCommand<Void> {
    private int id;
    private long timeout;

    public Lock() {
    }

    public Lock(int id, long timeout) {
      this.id = id;
      this.timeout = timeout;
    }

    /**
     * Returns the lock ID.
     *
     * @return The lock ID.
     */
    public int id() {
      return id;
    }

    /**
     * Returns the try lock timeout.
     *
     * @return The try lock timeout in milliseconds.
     */
    public long timeout() {
      return timeout;
    }

    @Override
    public CompactionMode compaction() {
      return timeout > 0 ? CompactionMode.SEQUENTIAL : CompactionMode.QUORUM;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      buffer.writeInt(id).writeLong(timeout);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      id = buffer.readInt();
      timeout = buffer.readLong();
    }
  }

  /**
   * Unlock command.
   */
  public static class Unlock extends LockCommand<Void> {
    private int id;

    public Unlock() {
    }

    public Unlock(int id) {
      this.id = id;
    }

    /**
     * Returns the lock ID.
     *
     * @return The lock ID.
     */
    public int id() {
      return id;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.SEQUENTIAL;
    }

    @Override
    public void writeObject(BufferOutput buffer, Serializer serializer) {
      buffer.writeInt(id);
    }

    @Override
    public void readObject(BufferInput buffer, Serializer serializer) {
      id = buffer.readInt();
    }
  }

  /**
   * Lock event.
   */
  public static class LockEvent implements CatalystSerializable {
    private int id;
    private long version;

    public LockEvent() {
    }

    public LockEvent(int id, long version) {
      this.id = id;
      this.version = version;
    }

    /**
     * Returns the lock ID.
     *
     * @return The lock ID.
     */
    public int id() {
      return id;
    }

    /**
     * Returns the lock version.
     *
     * @return The lock version.
     */
    public long version() {
      return version;
    }

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      buffer.writeInt(id).writeLong(version);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      id = buffer.readInt();
      version = buffer.readLong();
    }

    @Override
    public String toString() {
      return String.format("%s[id=%d, version=%d]", getClass().getSimpleName(), id, version);
    }
  }

  /**
   * Lock command type resolver.
   */
  public static class TypeResolver implements SerializableTypeResolver {
    @Override
    public void resolve(SerializerRegistry registry) {
      registry.register(Lock.class, -143);
      registry.register(Unlock.class, -144);
      registry.register(LockEvent.class, -145);
    }
  }

}
