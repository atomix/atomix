// SPDX-FileCopyrightText: 2017-present Open Networking Foundation <info@opennetworking.org>
//
// SPDX-License-Identifier: Apache-2.0

package io.atomix.storage.journal;

import java.io.Closeable;

/**
 * Journal.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Journal<E> extends Closeable {

  /**
   * Returns the journal writer.
   *
   * @return The journal writer.
   */
  JournalWriter<E> writer();

  /**
   * Opens a new journal reader.
   *
   * @param index The index at which to start the reader.
   * @return A new journal reader.
   */
  JournalReader<E> openReader(long index);

  /**
   * Opens a new journal reader.
   *
   * @param index The index at which to start the reader.
   * @param mode the reader mode
   * @return A new journal reader.
   */
  JournalReader<E> openReader(long index, JournalReader.Mode mode);

  /**
   * Returns a boolean indicating whether the journal is open.
   *
   * @return Indicates whether the journal is open.
   */
  boolean isOpen();

  @Override
  void close();
}
