/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.protocols.raft.server.storage.util;

import io.atomix.protocols.raft.server.storage.Indexed;
import io.atomix.protocols.raft.server.storage.entry.Entry;

/**
 * Log entry buffer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class EntryBuffer {
  private final Indexed<?>[] buffer;

  @SuppressWarnings("unchecked")
  public EntryBuffer(int size) {
    this.buffer = new Indexed[size];
  }

  /**
   * Appends an entry to the buffer.
   *
   * @param entry The entry to append.
   * @return The entry buffer.
   */
  public EntryBuffer append(Indexed<?> entry) {
    buffer[offset(entry.index())] = entry;
    return this;
  }

  /**
   * Looks up an entry in the buffer.
   *
   * @param index The entry index.
   * @param <T> The entry type.
   * @return The entry or {@code null} if the entry is not present in the index.
   */
  @SuppressWarnings("unchecked")
  public <T extends Entry<T>> Indexed<T> get(long index) {
    Indexed<?> entry = buffer[offset(index)];
    return entry != null && entry.index() == index ? (Indexed<T>) entry : null;
  }

  /**
   * Clears the buffer and resets the index to the given index.
   *
   * @return The entry buffer.
   */
  public EntryBuffer clear() {
    for (int i = 0; i < buffer.length; i++) {
      buffer[i] = null;
    }
    return this;
  }

  /**
   * Returns the buffer index for the given offset.
   */
  private int offset(long index) {
    int offset = (int) (index % buffer.length);
    if (offset < 0) {
      offset += buffer.length;
    }
    return offset;
  }
}
