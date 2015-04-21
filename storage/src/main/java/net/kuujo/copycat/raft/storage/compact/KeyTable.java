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
package net.kuujo.copycat.raft.storage.compact;

import net.kuujo.copycat.io.Buffer;
import net.kuujo.copycat.io.Bytes;
import net.kuujo.copycat.io.NativeBytes;
import net.kuujo.copycat.io.util.HashFunction;
import net.kuujo.copycat.io.util.HashFunctions;

/**
 * Log key lookup table.
 * <p>
 * This is a basic lookup table implementation designed to support efficient duplication of entry keys. When a key is
 * written to the table via {@link KeyTable#update(net.kuujo.copycat.io.Buffer, int)} the key is hashed and its slot located. If an index has
 * already been stored in the key's slot, the updated key's {@code offset} is compared with the stored offset. If the
 * updated offset is greater than the stored offset, the offset is updated, otherwise it's ignored.
 * <p>
 * Each entry in the table consumes 12 bytes for the 64-bit key hash and 32-bit offset.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KeyTable implements AutoCloseable {
  private static final int KEY_SIZE = 8;
  private static final int OFFSET_SIZE = 4;
  private static final int ENTRY_SIZE = KEY_SIZE + OFFSET_SIZE;
  private static final double PADDING = .15;

  private final Bytes bytes;
  private final HashFunction hashFunction;
  private int entries;
  private int size;

  public KeyTable(int entries) {
    this(entries, HashFunctions.CITYHASH);
  }

  public KeyTable(int entries, HashFunction hashFunction) {
    if (entries <= 0)
      throw new IllegalArgumentException("entry count must be positive");
    entries = (int) Math.ceil(entries * (1 + PADDING) / (double) ENTRY_SIZE) * ENTRY_SIZE;
    this.bytes = NativeBytes.allocate((long) entries * ENTRY_SIZE);
    this.hashFunction = hashFunction;
    this.entries = entries;
    bytes.zero();
  }

  /**
   * Returns the number of keys in the table.
   *
   * @return The number of keys in the table.
   */
  public int size() {
    return size;
  }

  /**
   * Hashes the given key.
   */
  private long hash(Buffer key) {
    return Math.abs(hashFunction.hash64(key));
  }

  /**
   * Updates the last index for the given key.
   *
   * @param key The key to update.
   * @param offset The offset of the key.
   * @return The offset of the previous entry with the given key.
   */
  public int update(Buffer key, int offset) {
    long hash = hash(key);
    long i = 0;
    long position = (hash % entries) * ENTRY_SIZE;
    while (isOccupied(position)) {
      if (bytes.readLong(position) == hash) {
        int previous = bytes.readInt(position + KEY_SIZE);
        if (previous < offset) {
          bytes.writeInt(position + KEY_SIZE, offset);
          return previous;
        }
        return -1;
      }
      position = ((hash + ++i) % entries) * ENTRY_SIZE;
    }

    bytes.writeLong(position, hash).writeInt(position + KEY_SIZE, offset);
    size++;
    return -1;
  }

  /**
   * Looks up the last index of the given key.
   *
   * @param key The key bytes.
   * @return The last index of the given key or {@code -1} if the key is not found.
   */
  public int lookup(Buffer key) {
    long hash = hash(key);
    long position = (hash % entries) * ENTRY_SIZE;
    while (isOccupied(position)) {
      if (bytes.readLong(position) == hash) {
        return bytes.readInt(position + KEY_SIZE);
      }
      position = (++hash % entries) * ENTRY_SIZE;
    }
    return -1;
  }

  /**
   * Returns a boolean value indicating whether the table contains the given key.
   *
   * @param key The key to check.
   * @return Indicates whether the table contains the given key.
   */
  public boolean contains(Buffer key) {
    long hash = hash(key);
    long position = (hash % entries) * ENTRY_SIZE;
    while (isOccupied(position)) {
      if (bytes.readLong(position) == hash) {
        return true;
      }
      position = (++hash % entries) * ENTRY_SIZE;
    }
    return false;
  }

  /**
   * Returns a boolean value indicating whether the current slot is open.
   */
  private boolean isOccupied(long position) {
    return bytes.readLong(position) != 0 || bytes.readInt(position + KEY_SIZE) != 0;
  }

  @Override
  public void close() {
    bytes.close();
  }

}
