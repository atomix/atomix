/*
 * Copyright 2017-present Open Networking Foundation
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
package io.atomix.storage.journal;

import java.util.Iterator;

/**
 * Log reader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface JournalReader<E> extends Iterator<Indexed<E>>, AutoCloseable {

  /**
   * Raft log reader mode.
   */
  enum Mode {

    /**
     * Reads all entries from the log.
     */
    ALL,

    /**
     * Reads committed entries from the log.
     */
    COMMITS,
  }

  /**
   * Returns the first index in the journal.
   *
   * @return the first index in the journal
   */
  long getFirstIndex();

  /**
   * Returns the current reader index.
   *
   * @return The current reader index.
   */
  long getCurrentIndex();

  /**
   * Returns the last read entry.
   *
   * @return The last read entry.
   */
  Indexed<E> getCurrentEntry();

  /**
   * Returns the next reader index.
   *
   * @return The next reader index.
   */
  long getNextIndex();

  /**
   * Returns whether the reader has a next entry to read.
   *
   * @return Whether the reader has a next entry to read.
   */
  @Override
  boolean hasNext();

  /**
   * Returns the next entry in the reader.
   *
   * @return The next entry in the reader.
   */
  @Override
  Indexed<E> next();

  /**
   * Resets the reader to the start.
   */
  void reset();

  /**
   * Resets the reader to the given index.
   *
   * @param index The index to which to reset the reader.
   */
  void reset(long index);

  @Override
  void close();
}
