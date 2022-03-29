// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal;

/**
 * Log writer.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface JournalWriter<E> extends AutoCloseable {

  /**
   * Returns the last written index.
   *
   * @return The last written index.
   */
  long getLastIndex();

  /**
   * Returns the last entry written.
   *
   * @return The last entry written.
   */
  Indexed<E> getLastEntry();

  /**
   * Returns the next index to be written.
   *
   * @return The next index to be written.
   */
  long getNextIndex();

  /**
   * Appends an entry to the journal.
   *
   * @param entry The entry to append.
   * @return The appended indexed entry.
   */
  <T extends E> Indexed<T> append(T entry);

  /**
   * Appends an indexed entry to the log.
   *
   * @param entry The indexed entry to append.
   */
  void append(Indexed<E> entry);

  /**
   * Commits entries up to the given index.
   *
   * @param index The index up to which to commit entries.
   */
  void commit(long index);

  /**
   * Resets the head of the journal to the given index.
   *
   * @param index the index to which to reset the head of the journal
   */
  void reset(long index);

  /**
   * Truncates the log to the given index.
   *
   * @param index The index to which to truncate the log.
   */
  void truncate(long index);

  /**
   * Flushes written entries to disk.
   */
  void flush();

  @Override
  void close();
}
