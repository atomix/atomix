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
package net.kuujo.copycat.protocol.raft.storage;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.HeapBuffer;
import net.kuujo.copycat.io.serializer.Writable;
import net.kuujo.copycat.io.util.ReferenceCounted;
import net.kuujo.copycat.io.util.ReferenceManager;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Raft log entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RaftEntry implements ReferenceCounted<RaftEntry>, Writable {

  /**
   * Raft entry type.
   */
  public static enum Type {

    /**
     * No-op entry.
     */
    NOOP(0),

    /**
     * Command entry.
     */
    COMMAND(1),

    /**
     * Tomb stone entry.
     */
    TOMBSTONE(-1);

    /**
     * Returns the entry type for the given identifier.
     *
     * @param id The entry type identifier.
     * @return The entry type.
     * @throws IllegalArgumentException If the entry type identifier is invalid.
     */
    public static Type forId(int id) {
      switch (id) {
        case 0:
          return NOOP;
        case 1:
          return COMMAND;
        case -1:
          return TOMBSTONE;
      }
      throw new IllegalArgumentException("invalid entry type identifier");
    }

    private final byte id;

    private Type(int id) {
      this.id = (byte) id;
    }

    /**
     * Returns the entry type identifier.
     *
     * @return The 1-byte entry type identifier.
     */
    public byte id() {
      return id;
    }
  }

  private final ReferenceManager<RaftEntry> referenceManager;
  private final AtomicInteger references = new AtomicInteger();
  private boolean readOnly;
  private Type type;
  private long index;
  private long term;
  private final Buffer key = HeapBuffer.allocate(1024, 1024 * 1024);
  private final Buffer entry = HeapBuffer.allocate(1024, 1024 * 1024);

  public RaftEntry(ReferenceManager<RaftEntry> referenceManager) {
    this.referenceManager = referenceManager;
  }

  /**
   * Initializes the entry.
   */
  void init(long index) {
    this.index = index;
    this.key.clear();
    this.entry.clear();
    readOnly = false;
  }

  @Override
  public RaftEntry acquire() {
    references.incrementAndGet();
    return this;
  }

  @Override
  public void release() {
    if (references.decrementAndGet() == 0)
      close();
  }

  @Override
  public int references() {
    return references.get();
  }

  /**
   * Returns the entry size.
   *
   * @return The entry size.
   */
  public int size() {
    return (int) (key.remaining() + entry.remaining());
  }

  /**
   * Returns the entry index.
   *
   * @return The entry index.
   */
  public long index() {
    return index;
  }

  /**
   * Reads the entry type.
   *
   * @return The entry type.
   */
  public Type readType() {
    return type;
  }

  /**
   * Writes the entry type.
   *
   * @param type The entry type.
   * @return The Raft entry.
   */
  public RaftEntry writeType(Type type) {
    if (readOnly)
      throw new IllegalStateException("entry is read-only");
    this.type = type;
    return this;
  }

  /**
   * Reads the entry term.
   *
   * @return The entry term.
   */
  public long readTerm() {
    return term;
  }

  /**
   * Writes the entry term.
   *
   * @param term The entry term.
   * @return The Raft entry.
   */
  public RaftEntry writeTerm(long term) {
    if (readOnly)
      throw new IllegalStateException("entry is read-only");
    this.term = term;
    return this;
  }

  /**
   * Reads the entry key.
   *
   * @param buffer The buffer into which to read the key.
   * @return The entry.
   */
  public RaftEntry readKey(Buffer buffer) {
    key.mark().read(buffer).reset();
    return this;
  }

  /**
   * Writes the entry key.
   *
   * @param buffer The buffer to write to the entry key.
   * @return The entry.
   */
  public RaftEntry writeKey(Buffer buffer) {
    if (readOnly)
      throw new IllegalStateException("entry is read-only");
    key.write(buffer);
    return this;
  }

  /**
   * Reads the entry value.
   *
   * @param buffer The buffer into which to read the value.
   * @return The entry.
   */
  public RaftEntry readEntry(Buffer buffer) {
    entry.mark().read(buffer).reset();
    return this;
  }

  /**
   * Writes the entry value.
   *
   * @param buffer The buffer to write to the entry value.
   * @return The entry.
   */
  public RaftEntry writeEntry(Buffer buffer) {
    if (readOnly)
      throw new IllegalStateException("entry is read-only");
    entry.write(buffer);
    return this;
  }

  /**
   * Reads the entry into the given entry.
   *
   * @param entry The entry into which to read the entry.
   * @return This entry.
   */
  public RaftEntry read(RaftEntry entry) {
    entry.type = type;
    entry.term = term;
    entry.key.write(key);
    entry.entry.write(key);
    return this;
  }

  /**
   * Writes the given entry into this entry.
   *
   * @param entry The entry to write to this entry.
   * @return This entry.
   */
  public RaftEntry write(RaftEntry entry) {
    if (readOnly)
      throw new IllegalStateException("entry is read-only");
    type = entry.type;
    term = entry.term;
    key.write(entry.key);
    this.entry.write(entry.entry);
    return this;
  }

  /**
   * Returns a boolean value indicating whether the entry is read-only.
   *
   * @return Indicates whether the entry is read-only.
   */
  public boolean isReadOnly() {
    return readOnly;
  }

  /**
   * Sets the entry to read-only mode.
   *
   * @return The read-only entry.
   */
  public RaftEntry asReadOnlyEntry() {
    key.flip();
    entry.flip();
    readOnly = true;
    return this;
  }

  @Override
  public void writeObject(Buffer buffer) {
    if (!readOnly)
      throw new IllegalStateException("entry must be read-only");
    buffer.writeByte(type.id)
      .writeLong(term)
      .writeInt((int) key.remaining())
      .write(key)
      .writeInt((int) entry.remaining())
      .write(entry);
  }

  @Override
  public void readObject(Buffer buffer) {
    type = Type.forId(buffer.readByte());
    term = buffer.readLong();
    int keySize = buffer.readInt();
    buffer.read(key.limit(keySize));
    int entrySize = buffer.readInt();
    buffer.read(entry.limit(entrySize));
    asReadOnlyEntry();
  }

  @Override
  public void close() {
    key.close();
    entry.close();
    referenceManager.release(this);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, type=%s, term=%d]", getClass().getSimpleName(), index, type, term);
  }

}
