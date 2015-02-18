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
package net.kuujo.copycat.raft.log;

import net.kuujo.copycat.util.internal.Assert;

import java.nio.ByteBuffer;

/**
 * Raft log entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftEntry {

  /**
   * Raft entry type.
   */
  public static enum Type {
    CONFIGURATION((byte) 0),
    COMMAND((byte) 1),
    SNAPSHOT((byte) 2);

    private final byte id;

    private Type(byte id) {
      this.id = id;
    }

    /**
     * Returns the entry type ID.
     *
     * @return The entry type ID.
     */
    public byte id() {
      return id;
    }

    /**
     * Looks up the type for the given type ID.
     *
     * @param type The type ID for the type to look up.
     * @return The related type.
     */
    public static Type lookup(byte type) {
      switch (type) {
        case 0:
          return CONFIGURATION;
        case 1:
          return COMMAND;
        default:
          throw new IllegalStateException();
      }
    }
  }

  private ByteBuffer buffer;

  public RaftEntry() {
  }

  public RaftEntry(ByteBuffer buffer) {
    this.buffer = Assert.notNull(buffer, "buffer");
  }

  public RaftEntry(Type type, long term, ByteBuffer entry) {
    buffer = ByteBuffer.allocate(9 + entry.remaining());
    buffer.put(type.id);
    buffer.putLong(term);
    buffer.put(entry);
    buffer.flip();
  }

  /**
   * Returns the entry type.
   *
   * @return The entry type.
   */
  public Type type() {
    buffer.rewind();
    return Type.lookup(buffer.get());
  }

  /**
   * Returns the entry term.
   *
   * @return The entry term.
   */
  public long term() {
    buffer.position(1);
    return buffer.getLong();
  }

  /**
   * Returns the entry buffer.
   *
   * @return The entry buffer.
   */
  public ByteBuffer entry() {
    buffer.position(9);
    return buffer.slice();
  }

  /**
   * Returns the complete entry buffer size.
   *
   * @return The complete entry buffer size.
   */
  public int size() {
    return buffer.limit();
  }

  /**
   * Returns the entry buffer.
   *
   * @return The entry buffer.
   */
  public ByteBuffer buffer() {
    buffer.rewind();
    return buffer;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof RaftEntry && ((RaftEntry) object).buffer.equals(buffer);
  }

  @Override
  public int hashCode() {
    int hashCode = 17;
    hashCode = 37 * hashCode + buffer.hashCode();
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("RaftEntry[type=%s, term=%d, entry=%s]", type(), term(), entry());
  }

}
