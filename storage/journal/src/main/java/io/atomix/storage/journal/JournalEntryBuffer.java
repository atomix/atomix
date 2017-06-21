/*
 * Copyright 2016-present Open Networking Laboratory
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
package io.atomix.storage.journal;

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Log entry buffer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class JournalEntryBuffer<E> {
  private final AtomicReferenceArray<Indexed<E>> buffer;
  private final int size;

  @SuppressWarnings("unchecked")
  JournalEntryBuffer(int size) {
    this.buffer = new AtomicReferenceArray<>(size);
    this.size = size;
  }

  /**
   * Appends an entry to the buffer.
   *
   * @param entry The entry to append.
   * @return The entry buffer.
   */
  public JournalEntryBuffer append(Indexed<E> entry) {
    buffer.set(offset(entry.index()), entry);
    return this;
  }

  /**
   * Looks up an entry in the buffer.
   *
   * @param index The entry index.
   * @return The entry or {@code null} if the entry is not present in the index.
   */
  public Indexed<E> get(long index) {
    Indexed<E> entry = buffer.get(offset(index));
    return entry != null && entry.index() == index ? entry : null;
  }

  /**
   * Clears the buffer.
   */
  public void clear() {
    for (int i = 0; i < buffer.length(); i++) {
      buffer.set(i, null);
    }
  }

  /**
   * Returns the buffer index for the given offset.
   */
  private int offset(long index) {
    return (int) (index % size);
  }
}
