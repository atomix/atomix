// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

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
