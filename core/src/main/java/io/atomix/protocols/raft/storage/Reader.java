/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.protocols.raft.storage;

import io.atomix.protocols.raft.storage.entry.Entry;

import java.util.Iterator;

/**
 * Log reader.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Reader extends Iterator<Indexed<? extends Entry<?>>>, AutoCloseable {

  /**
   * Reader mode.
   */
  enum Mode {
    /**
     * Reads all entries.
     */
    ALL,

    /**
     * Reads only committed entries.
     */
    COMMITS,
  }

  /**
   * Returns the reader mode.
   *
   * @return The reader mode.
   */
  Mode mode();

  /**
   * Returns the current reader index.
   *
   * @return The current reader index.
   */
  long currentIndex();

  /**
   * Returns the last read entry.
   *
   * @return The last read entry.
   */
  Indexed<? extends Entry<?>> currentEntry();

  /**
   * Returns the next reader index.
   *
   * @return The next reader index.
   */
  long nextIndex();

  /**
   * Returns the entry at the given index.
   *
   * @param index The entry index.
   * @param <T>   The entry type.
   * @return The entry at the given index or {@code null} if the entry doesn't exist.
   * @throws IndexOutOfBoundsException if the given index is outside the range of the log
   */
  <T extends Entry<T>> Indexed<T> get(long index);

  /**
   * Resets the reader to the given index.
   *
   * @param index The index to which to reset the reader.
   * @return The last entry read at the reset index.
   */
  Indexed<? extends Entry<?>> reset(long index);

  /**
   * Resets the reader to the start.
   */
  void reset();

  @Override
  void close();
}