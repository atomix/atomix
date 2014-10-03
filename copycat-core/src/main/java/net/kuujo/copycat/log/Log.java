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
   * Opens the log.
   *
   * @throws IOException If the log could not be opened.
   */
  void open() throws IOException;

  /**
   * Returns the current size of the log.
   *
   * @return The current size of the log.
   */
  long size();

  /**
   * Returns a boolean indicating whether the log is empty.
   *
   * @return Indicates whether the log is empty.
   */
  boolean isEmpty();

  /**
   * Appends an entry to the log.
   *
   * @param entry The entry to append.
   * @return The appended entry index.
   */
  long appendEntry(Entry entry);

  /**
   * Appends a list of entries to the log.
   *
   * @param entries A list of entries to append to the log.
   * @return A list of appended entry indices.
   */
  List<Long> appendEntries(Entry... entries);

  /**
   * Appends a list of entries to the log.
   *
   * @param entries A list of entries to append to the log.
   * @return A list of appended entry indices.
   */
  List<Long> appendEntries(List<Entry> entries);

  /**
   * Returns a boolean indicating whether the log contains an entry.
   *
   * @param index The index for which to search the log.
   * @return Indicates whether the log contains an entry at the given index.
   */
  boolean containsEntry(long index);

  /**
   * Returns the first index in the log.
   *
   * @return The first index in the log.
   */
  long firstIndex();

  /**
   * Returns the first entry in the log.
   *
   * @return The first entry in the log.
   */
  <T extends Entry> T firstEntry();

  /**
   * Returns the last index in the log.
   *
   * @return The last index in the log.
   */
  long lastIndex();

  /**
   * Returns the last entry in the log.
   *
   * @return The last entry in the log.
   */
  <T extends Entry> T lastEntry();

  /**
   * Returns a log entry by index.
   *
   * @param index The index from which to retrieve the entry.
   * @return The log entry.
   */
  <T extends Entry> T getEntry(long index);

  /**
   * Returns a list of entries from the log.
   *
   * @param from The index at which to start.
   * @param to The index at which to end.
   * @return A list of log entries.
   */
  <T extends Entry> List<T> getEntries(long from, long to);

  /**
   * Removes the entry at the given index.
   *
   * @param index The index at which to remove the entry.
   */
  void removeEntry(long index);

  /**
   * Removes all entries after the given index.
   *
   * @param index The index after which to remove entries.
   */
  void removeAfter(long index);

  /**
   * Forces the log to sync to disk (if the log is persistent).
   *
   * @throws IOException If the log file could not be synced.
   */
  void sync() throws IOException;

  /**
   * Closes the log.
   *
   * @throws IOException If the log file could not be closed.
   */
  void close() throws IOException;

  /**
   * Deletes the log.
   *
   * @throws IOException If the log file could not be deleted.
   */
  void delete() throws IOException;

}
