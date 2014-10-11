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

import java.io.IOException;
import java.util.List;

/**
 * Log.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Log {
  /**
   * Appends a list of entries to the log.
   *
   * @param entries A list of entries to append to the log.
   * @return A list of appended entry indices.
   * @throws NullPointerException if {@code entries} is null
   * @throws IllegalStateException if the log is not open
   */
  List<Long> appendEntries(Entry... entries);

  /**
   * Appends a list of entries to the log.
   *
   * @param entries A list of entries to append to the log.
   * @return A list of appended entry indices.
   * @throws NullPointerException if {@code entries} is null
   * @throws IllegalStateException if the log is not open
   */
  List<Long> appendEntries(List<Entry> entries);

  /**
   * Appends an entry to the log.
   *
   * @param entry The entry to append.
   * @return The appended entry index.
   * @throws NullPointerException if {@code entry} is null
   * @throws IllegalStateException if the log is not open
   */
  long appendEntry(Entry entry);

  /**
   * Closes the log.
   *
   * @throws IOException If the log file could not be closed.
   * @throws IllegalStateException if the log is not open
   */
  void close() throws IOException;

  /**
   * Returns a boolean indicating whether the log contains an entry.
   *
   * @param index The index for which to search the log.
   * @return Indicates whether the log contains an entry at the given index.
   * @throws IllegalStateException if the log is not open
   */
  boolean containsEntry(long index);

  /**
   * Deletes the log.
   *
   * @throws IOException If the log file could not be deleted.
   */
  void delete() throws IOException;

  /**
   * Returns the first entry in the log.
   *
   * @return The first entry in the log.
   * @throws IllegalStateException if the log is not open
   */
  <T extends Entry> T firstEntry();

  /**
   * Returns the first index in the log.
   *
   * @return The first index in the log.
   * @throws IllegalStateException if the log is not open
   */
  long firstIndex();

  /**
   * Returns a list of entries from the log.
   *
   * @param from The index at which to start, inclusive.
   * @param to The index at which to end, inclusive.
   * @return A list of log entries.
   * @throws LogIndexOutOfBoundsException if {@code from} or {@code to} represent a range that is
   *         out of bounds
   */
  <T extends Entry> List<T> getEntries(long from, long to);

  /**
   * Returns a log entry by index.
   *
   * @param index The index from which to retrieve the entry.
   * @return The log entry.
   * @throws IllegalStateException if the log is not open
   */
  <T extends Entry> T getEntry(long index);

  /**
   * Returns a boolean indicating whether the log is empty.
   *
   * @return Indicates whether the log is empty.
   * @throws IllegalStateException if the log is not open
   */
  boolean isEmpty();

  /**
   * Returns whether the log is open or not.
   * 
   * @return true if the log is open
   * @throws IllegalStateException if the log is not open
   */
  boolean isOpen();

  /**
   * Returns the last entry in the log.
   *
   * @return The last entry in the log.
   * @throws IllegalStateException if the log is not open
   */
  <T extends Entry> T lastEntry();

  /**
   * Returns the last index in the log.
   *
   * @return The last index in the log.
   * @throws IllegalStateException if the log is not open
   */
  long lastIndex();

  /**
   * Opens the log.
   *
   * @throws IOException If the log could not be opened.
   * @throws IllegalStateException if the log is already open
   */
  void open() throws IOException;

  /**
   * Removes all entries after the given index, exclusive.
   *
   * @param index The index after which to remove entries.
   * @throws IllegalStateException if the log is not open
   */
  void removeAfter(long index);

  /**
   * Returns the current size of the log.
   *
   * @return The current size of the log.
   * @throws IllegalStateException if the log is not open
   */
  long size();

  /**
   * Forces the log to sync to disk (if the log is persistent).
   *
   * @throws IOException If the log file could not be synced.
   * @throws IllegalStateException if the log is not open
   */
  void sync() throws IOException;
}
