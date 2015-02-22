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
package net.kuujo.copycat.log.compaction;

import net.kuujo.copycat.util.internal.Bytes;
import net.kuujo.copycat.util.internal.Hash;

import java.nio.ByteBuffer;

/**
 * Buffered lookup strategy implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class BufferedLookupStrategy implements LookupStrategy {
  private static final int KEY_SIZE = 4;
  private static final int ENTRY_SIZE = KEY_SIZE + 8; // Hash int plus index.
  private final int slots;
  private final ByteBuffer buffer;

  public BufferedLookupStrategy(int entryLimit) {
    this.slots = entryLimit / 2;
    this.buffer = ByteBuffer.allocateDirect(slots * ENTRY_SIZE);
  }

  @Override
  public void put(ByteBuffer key, long index) {
    byte[] bytes = Bytes.getBytes(key);
    int i = 0;
    int hash = Math.abs(Hash.hash32(bytes));

    int position = (hash % slots) * ENTRY_SIZE;
    while (isOccupied(position)) {
      if (buffer.getInt(position) == hash) {
        buffer.putLong(position + KEY_SIZE, index);
        return;
      }
      position = ((hash + ++i) % slots) * ENTRY_SIZE;
    }

    buffer.putInt(position, hash);
    buffer.putLong(position + KEY_SIZE, index);
  }

  @Override
  public Long get(ByteBuffer key) {
    byte[] bytes = Bytes.getBytes(key);
    int i = 0;
    int hash = Math.abs(Hash.hash32(bytes));

    int position = (hash % slots) * ENTRY_SIZE;
    while (isOccupied(position)) {
      if (buffer.getInt(position) == hash) {
        return buffer.getLong(position + KEY_SIZE);
      }
      position = ((hash + ++i) % slots) * ENTRY_SIZE;
    }
    return null;
  }

  /**
   * Returns a boolean value indicating whether the current slot is open.
   */
  private boolean isOccupied(int position) {
    return buffer.getInt(position) != 0 && buffer.getLong(position + KEY_SIZE) != 0;
  }

}
