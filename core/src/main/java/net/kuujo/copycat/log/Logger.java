/*
 * Copyright 2014 the original author or authors.
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
package net.kuujo.copycat.log;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Logger.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Logger extends Closeable {

  /**
   * Opens the logger.
   */
  void open() throws IOException;

  /**
   * Returns a boolean indicating whether the log is open.
   *
   * @return Indicates whether the log is open.
   */
  boolean isOpen();

  /**
   * Returns the logger size.
   *
   * @return The logger size.
   * @throws java.lang.IllegalStateException If the log is not open.
   */
  long size();

  /**
   * Returns a boolean indicating whether the logger is empty.
   *
   * @return Indicates whether the logger is empty.
   */
  boolean isEmpty();

  /**
   * Appends an entry to the logger.
   *
   * @param entry The entry to append.
   * @return The appended entry index.
   * @throws java.lang.IllegalStateException If the log is not open.
   * @throws java.lang.NullPointerException If the entry is null.
   */
  long appendEntry(ByteBuffer entry);

  /**
   * Appends a list of entries to the log.
   *
   * @param entries A list of entries to append.
   * @return A list of appended entry indices.
   * @throws java.lang.IllegalStateException If the log is not open.
   * @throws java.lang.NullPointerException If the entries list is null.
   */
  List<Long> appendEntries(List<ByteBuffer> entries);

  /**
   * Returns the index of the first entry in the log.
   *
   * @return The index of the first entry in the log or {@code null} if the log is empty.
   * @throws java.lang.IllegalStateException If the log is not open.
   */
  Long firstIndex();

  /**
   * Returns the index of the last entry in the log.
   *
   * @return The index of the last entry in the log or {@code null} if the log is empty.
   * @throws java.lang.IllegalStateException If the log is not open.
   */
  Long lastIndex();

  /**
   * Returns a boolean indicating whether the log contains an entry at the given index.
   *
   * @param index The index of the entry to check.
   * @return Indicates whether the log contains the given index.
   * @throws java.lang.IllegalStateException If the log is not open.
   */
  boolean containsIndex(long index);

  /**
   * Gets an entry from the log.
   *
   * @param index The index of the entry to get.
   * @return The entry at the given index, or {@code null} if the entry doesn't exist.
   * @throws java.lang.IllegalStateException If the log is not open.
   */
  ByteBuffer getEntry(long index);

  /**
   * Gets a list of entries from the log.
   *
   * @param from The index of the start of the list of entries to get.
   * @param to The index of the end of the list of entries to get.
   * @return A list of entries from the given start index to the given end index.
   * @throws java.lang.IllegalStateException If the log is not open.
   */
  List<ByteBuffer> getEntries(long from, long to);

  /**
   * Removes all entries after the given index.
   *
   * @param index The index after which to remove entries.
   * @throws java.lang.IllegalStateException If the log is not open.
   */
  void removeAfter(long index);

  /**
   * Compacts the log at the given index, appending the given entry.
   *
   * @param index The index at which to compact the log.
   */
  void compact(long index);

  /**
   * Compacts the log at the given index, appending the given entry.
   *
   * @param index The index at which to compact the log.
   * @param entry The entry to write to the log at the given index.
   */
  void compact(long index, ByteBuffer entry);

  /**
   * Flushes the log to disk.
   *
   * @throws java.lang.IllegalStateException If the log is not open.
   */
  void flush();

  /**
   * Flushes the log to disk, optionally forcing the flush.
   *
   * @param force Whether to force the log to be flushed to disk even if the flush is blocked by configuration.
   */
  void flush(boolean force);

  /**
   * Closes the logger.
   */
  @Override
  void close() throws IOException;

  /**
   * Returns a boolean indicating whether the log is closed.
   *
   * @return Indicates whether the log is closed.
   */
  boolean isClosed();

  /**
   * Deletes the logger.
   */
  void delete();

}
